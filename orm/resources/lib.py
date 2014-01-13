from base import DeclarativeMetaclass, GenericResource
from django.conf.urls import url
from django.http.response import HttpResponse
from django.utils.log import logging
from decorator import log_time
from tastypie import fields, http
from tastypie.exceptions import ImmediateHttpResponse
from tastypie.utils.urls import trailing_slash
import datetime
import logging


LOOKUP_SEP = '__'
DJANGO_TO_MONGO_MAPPER = {"lt" : "$lt", "lte" : "$lte", "gt" : "$gt", "gte" : "$gte", "ne" : "$ne", "exists":"$exists","in":"$in","notin" :"$nin"}    
logger = logging.getLogger("restify")

class MongoResource(GenericResource):
    __metaclass__ = DeclarativeMetaclass

    def __init__(self,api_name=None,model=None,related_daos = {}):
        super(MongoResource,self).__init__(api_name)
        self.model = model
        
    def base_urls(self):
        """
        The standard URLs this ``Resource`` should respond to.
        """
        return [
            url(r"^(?P<resource_name>%s)%s$" % (self._meta.resource_name, trailing_slash()), self.wrap_view('dispatch_list'), name="api_dispatch_list"),
            url(r"^(?P<resource_name>%s)/schema%s$" % (self._meta.resource_name, trailing_slash()), self.wrap_view('get_schema'), name="api_get_schema"),
            url(r"^(?P<resource_name>%s)/set/(?P<%s_list>\w[\w/;-]*)%s$" % (self._meta.resource_name, self._meta.detail_uri_name, trailing_slash()), self.wrap_view('get_multiple'), name="api_get_multiple"),
            url(r"^(?P<resource_name>%s)/(?P<%s>\w[\w-]*)%s$" % (self._meta.resource_name, self._meta.detail_uri_name, trailing_slash()), self.wrap_view('dispatch_detail'), name="api_dispatch_detail"),
        ]

    @log_time
    def get_object_list(self, client, applicable_filters={},fields=[], **kwargs):
        return self.model.get_filtered(client,filters=applicable_filters,fields=fields,**kwargs)
    
    @log_time
    def create_obj(self,client,data,**kwargs):
        now = datetime.datetime.utcnow().isoformat()
        data["modifiedAt"] = now
        data["createdAt"] = now
        return self.model.create(client,data,**kwargs)
    
    @log_time
    def get_obj(self,client,res_id,**kwargs):
        return self.model.get(client,doc_id=res_id,fields=[],**kwargs)
    
    @log_time
    def delete_obj(self,client,doc_id,**kwargs):
        return self.model.delete(client,doc_id=doc_id,**kwargs)
    
    @log_time
    def update_obj(self,client,res_id,data,**kwargs):
        now = datetime.datetime.utcnow().isoformat()
        data["modifiedAt"] = now
        return self.model.update(client,doc_id=res_id,data=data,**kwargs)
    
    def hydrate(self, bundle):
        """
        A hook to allow an initial manipulation of data before all methods/fields
        have built out the hydrated data.

        Useful if you need to access more than one hydrated field or want
        to annotate on additional data.

        Must return the modified bundle.
        """
        bundle.data["owner"] = bundle.request.user.get("id", None)
        return bundle
