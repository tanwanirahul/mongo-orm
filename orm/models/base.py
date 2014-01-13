'''
Created on 07-Oct-2013

@author: Rahul
'''
from django.core.exceptions import ImproperlyConfigured
import datetime
from constants import DJANGO_TO_MONGO_MAPPER, LOOKUP_SEP
from jsonschema.validators import validate
from jsonschema.exceptions import ValidationError
import orm.exceptions
import json
import collections


class ModelProperties(dict):
    '''
        The class holds all the configurable properties of a Model.
    '''
    def __init__(self,skip_validation=False,required_fields = [],is_list_type = False,wrapped_in=None):
        self.skip_validation = skip_validation
        self.required_fields = required_fields
        if not type(required_fields) in (list,):
            raise ImproperlyConfigured("Required fields attribute of Meta must be a list.")
        if is_list_type and wrapped_in is None:
            raise ImproperlyConfigured("If is_list_type is set to True, wrapped_in must be specified.")
        self.is_list_type = is_list_type
        self.wrapped_in = wrapped_in
        
    def __contains__(self,item):
        return hasattr(self, item)
    
    def __getitem__(self,item):
        value = getattr(self, item,None)
        if None:
            raise KeyError("Key %s not found in Model's Properties" %item)
        return value


class BaseModel(object):

    python_to_schema_mapper = {
                           int: "integer",
                           float :"number",
                           str : "string",
                           basestring : "string",
                           unicode : "string",
                           datetime.datetime : "string",
                           bool : "boolean"
                           }

    @classmethod
    def _python_to_jsonschema(cls,structure):
        if type(structure) in (dict,):
            json_struct ={"type" : "object", "properties" : {}}
            json_struct["properties"] = dict((name,BaseModel._python_to_jsonschema(value)) for name,value in structure.items())
            return json_struct
        elif structure in BaseModel.python_to_schema_mapper:
            return {"type" : BaseModel.python_to_schema_mapper[structure]}

    def _get_jsonschema(self):
        schema_type = getattr(self, "schema_type")
        if schema_type == "python":
            return self._python_to_jsonschema(self.structure)
        return self.structure

    @classmethod
    def _get_meta_attributes(cls,meta=None):
        attrs= {}
        for attribute in dir(meta):
            if not attribute.startswith("_"):
                attrs[attribute] = getattr(meta, attribute)
        return attrs

    def __init__(self,dao=None,activity_logger=None):
        super(BaseModel,self).__init__()
        self.dao = dao
        self.json_structure = self._get_jsonschema() 
        if not hasattr(self, "structure"):
            raise ImproperlyConfigured("Models schema must be specified. Define structure attribute in Model class for the same.")
        model_properties = {}
        meta = getattr(self, "Meta",None) 
        if meta:
            model_properties = BaseModel._get_meta_attributes(meta)
        skip_validation = getattr(self, "skip_validation",False)
        model_properties.update({"skip_validation" : skip_validation})
        self.properties = ModelProperties(**model_properties)

    def create(self,client,data,**kwargs):
        self.validate(data)
#        self.activity_logger.added(client,data)
        return self.dao.insert(data,**kwargs)

    def update(self,client,doc_id,data,**kwargs):
        self.validate(data)
#        self.activity_logger.updated(client,data)
        return self.dao.update_fields_by_id(doc_id,document=data,**kwargs)

    def get(self,client,doc_id,fields=[],**kwargs):
        data = self.dao.get_by_id(doc_id,fields=fields,**kwargs)
#        self.activity_logger.viewed(client,data)
        return data

    def delete(self,client,doc_id,**kwargs):
        data = self.dao.remove_by_id(doc_id,**kwargs)
#        self.activity_logger.viewed(client,data)
        return data

    def register_validation_schema(self,schema):
        self.json_structure = self._convert(schema)
    
    def _convert(self,data):
        if isinstance(data, basestring):
            return str(data)
        elif isinstance(data, collections.Mapping):
            return dict(map(self._convert, data.iteritems()))
        elif isinstance(data, collections.Iterable):
            return type(data)(map(self._convert, data))
        else:
            return data
        
    def get_filtered(self,client,filters ={},fields=[],**kwargs):
        if not self.properties["is_list_type"]:
            filters = self._build_filters(filters)
            data = self.dao.get_filtered(query=filters,fields=fields,**kwargs)
        else:
            data = self.dao.get_filtered(query={},fields=fields,**kwargs)
            data = self._in_memory_filters(filters,data)
        return data
    
    def validate(self,data):
        self.pre_validation(data,self.json_structure)
        if self.properties["skip_validation"]:
            return True
        try:
            validate(data,self.json_structure)
        except ValidationError as e:
            raise exceptions.ValidationError("The data specified is not adhering to the required schema. %s" %e.message)
    
    def pre_validation(self,data, json_structure):
        return True

    def _build_filters(self,filters={}):
        query_terms = DJANGO_TO_MONGO_MAPPER.keys()
        query_selectors = {}
        for filter_expr, value in filters.items():
            filter_bits = filter_expr.split(LOOKUP_SEP)
            field = filter_bits.pop(0)
            operator = "exact" if not filter_bits else filter_bits.pop(0)
            field_type = self._get_field_type(field)
            if not field_type:
                continue
            value = self._pre_process_special_cases(value)
            value = self.type_conversion(field_type,value)
            query = self.build_query(field,operator,value,field_type)
            for key,query_value in query.items():
                if key in query_selectors and type(query_value) in (dict,):
                    query_selectors[key].update(query_value)
                else:
                    query_selectors.update(query)
        return query_selectors
    
    def build_query(self,db_field_name,operator,value,value_type):
        if operator == "exact":
            return {db_field_name : value}
        elif operator in DJANGO_TO_MONGO_MAPPER.keys():
            operator = DJANGO_TO_MONGO_MAPPER[operator]
            if operator == "$regex":
                value = "/%s/i" %value 
            return {db_field_name : {operator : value}}
        return {}

    def _pre_process_special_cases(self,value):
        if value == "True" or value == "true" or value == True: 
            return True
        elif value == "False" or value == "false" or value == False:
            return False
        elif "[" in value and "]" in value: #Support for in and notin kind filters.
            return value[1:-1].split(",")
        elif value in ('nil', 'none', 'None', None):
            value = None
        return value

    def _get_field_type(self,field):
        field_bits  = field.split(".")
        main_field = field_bits.pop(0)
        main_field_type = self.structure.get(main_field,None)
        if not main_field_type:
            return None
        if not field_bits:
            return main_field_type
        nested_field = field_bits.pop(0)
        if not nested_field in main_field_type:
            return None
        sub_field_type=main_field_type.get(nested_field,None) 
        if sub_field_type:
            return sub_field_type

    def type_conversion(self,field_type, value):
        try:
            if field_type in (bool,) or isinstance(value,bool): #For boolean types, values are already pre_processed. Hence, no conversion.
                return value
            elif type(value) in (list,): #Support for in and notin kind filters.
                return [self.type_conversion(field_type, val) for val in value]
            elif field_type in (basestring,str,unicode):
                return unicode(value)
            elif field_type in (int,):
                return int(value)
            elif field_type in (float,):
                return float(value)
            elif field_type in (datetime.datetime,):
                return self.__str_to_datetime(value)
            else:
                return value
        except Exception as e:
            raise ValueError("Values passed in for filtering are not valid.")
        
    def __assert_type(self,data,data_type,attribute_name):
        if type(data) not in (data_type,) and not issubclass(type(data),data_type):
            raise ValidationError("Schema validation failed for field %s." %attribute_name)
        
    def __str_to_datetime(self,data,attribute_name=None):
        from dateutil import parser
        if type(data) in (datetime.datetime,):
            return data.isoformat()
        try:
            return parser.parse(data)
        except Exception as e:
            raise ValidationError("Datetime specified is not in ISO Format for field %s." %attribute_name)
