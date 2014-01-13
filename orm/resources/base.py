'''
Created on 20-Nov-2013

@author: Rahul
'''
from bson.errors import InvalidId
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.core.urlresolvers import NoReverseMatch
from django.utils.cache import patch_cache_control, patch_vary_headers
from mongokit.schema_document import RequireFieldError
from tastypie import fields, http
from tastypie.exceptions import NotFound, Unauthorized,BadRequest
from tastypie.resources import Resource, DeclarativeMetaclass, csrf_exempt
from tastypie.utils.mime import determine_format
from django.utils.log import logging
from tastypie.authorization import Authorization
from decorator import log_time
from pymongo.errors import DuplicateKeyError
from tastypie.bundle import Bundle
from orm.exceptions import ValidationError, DoesNotExist
from paginator import Paginator
from orm.utils import AttributeStyleDict
from tastypie.utils.dict import dict_strip_unicode_keys
import json

logger = logging.getLogger(__name__)

class DeclarativeMetaclass(DeclarativeMetaclass):
    def __new__(cls, name, bases, attrs):
        new_class = super(DeclarativeMetaclass, cls).__new__(cls, name, bases, attrs)
        new_class._meta.collection_name = 'data'
        new_class._meta.paginator_class= Paginator
        return new_class

class GenericResource(Resource):

    __metaclass__ = DeclarativeMetaclass
    
    def __init__(self,api_name=None,related_daos = {}):
        super(GenericResource,self).__init__(api_name)
        
    def wrap_view(self, view):
        # Overriding this method from Resource so that 
        # we can plugin a call to log_activity
        # This will also enable us to have custom error handling
        @csrf_exempt
        def wrapper(request, *args, **kwargs):
            try:
                callback = getattr(self, view)
                #self.log_activity(request, **kwargs)
                response = callback(request, *args, **kwargs)

                # Our response can vary based on a number of factors, use
                # the cache class to determine what we should ``Vary`` on so
                # caches won't return the wrong (cached) version.
                varies = getattr(self._meta.cache, "varies", [])

                if varies:
                    patch_vary_headers(response, varies)

                if self._meta.cache.cacheable(request, response):
                    if self._meta.cache.cache_control():
                        # If the request is cacheable and we have a
                        # ``Cache-Control`` available then patch the header.
                        patch_cache_control(response, **self._meta.cache.cache_control())

                if request.is_ajax() and not response.has_header("Cache-Control"):
                    # IE excessively caches XMLHttpRequests, so we're disabling
                    # the browser cache here.
                    # See http://www.enhanceie.com/ie/bugs.asp for details.
                    patch_cache_control(response, no_cache=True)

                return response
            
            except (BadRequest, fields.ApiFieldError,DoesNotExist, Unauthorized), e:
                logger.error("%s failed with bad request error %s" % (request.path, e), 
                             exc_info=False)
                data = {"error": e.args[0] if getattr(e, 'args') else ''}
                return self.error_response(request, data, response_class=http.HttpBadRequest)
            except ValidationError as e:
                logger.error("%s failed with validation error %s" % (request.path, e), 
                             exc_info=False)
                data = {"error": e.message}
                return self.error_response(request, data, response_class=http.HttpBadRequest)
            except Exception as e:
                logger.error("%s failed with unknown error %s" % (request.path, e),
                             exc_info=True)
                if hasattr(e, 'response'):
                    return e.response

                # A real, non-expected exception.
                # Handle the case where the full traceback is more helpful
                # than the serialized error.
                if settings.DEBUG and getattr(settings, 'TASTYPIE_FULL_DEBUG', False):
                    raise

                # Re-raise the error to get a proper traceback when the error
                # happend during a test case
                if request.META.get('SERVER_NAME') == 'testserver':
                    raise
                
                # Rather than re-raising, we're going to things similar to
                # what Django does. The difference is returning a serialized
                # error message.
                return self._handle_500(request, e)

        return wrapper
    
    def get_multiple(self, request, **kwargs):
        """
        Returns a serialized list of resources based on the identifiers
        from the URL.

        Calls ``obj_get`` to fetch only the objects requested. This method
        only responds to HTTP GET.

        Should return a HttpResponse (200 OK).
        """
        self.method_check(request, allowed=['get'])
        self.is_authenticated(request)
        self.throttle_check(request)

        # Rip apart the list then iterate.
        kwarg_name = '%s_list' % self._meta.detail_uri_name
        obj_identifiers = kwargs.get(kwarg_name, '').split(';')
        objects = []
        not_found = []
        base_bundle = self.build_bundle(request=request)

        for identifier in obj_identifiers:
            try:
                kwargs.update({self._meta.detail_uri_name: identifier})
                obj = self.obj_get(bundle=base_bundle, **kwargs)
                bundle = self.build_bundle(obj=obj, request=request)
                bundle = self.full_dehydrate(bundle)
                objects.append(bundle)
            except (ObjectDoesNotExist, Unauthorized):
                not_found.append(identifier)

        object_list = {
            self._meta.collection_name: objects,
        }

        if len(not_found):
            object_list['not_found'] = not_found

        self.log_throttled_access(request)
        return self.create_response(request, object_list)
    
    @classmethod
    def get_fields(cls, fields=None, excludes=None):
        return cls.fields
    
    def get_mongo_doc(self):
        return self.dao.__class__
    
    def create_identifier(self,name):
        identifier = ''.join(e for e in name if e.isalnum())
        return identifier.lower()
        
    def remove_api_resource_names(self, url_dict):
        # The method in the super class deletes the resource name from the dictionary
        # We are overriding this method so that it does not delete the resource name
        return url_dict

    def alter_detail_data_to_serialize(self, request, data):
        '''Wrap detail response in a data element'''
        return {"data": data}

    def determine_format(self, request):
        '''return application/json as the default format'''
        fmt = determine_format(request, self._meta.serializer, default_format=self._meta.default_format)
        if fmt == 'text/html':
            fmt = 'application/json'

        if request.GET.get('format', None) == 'html':
            fmt = 'text/html'

        return fmt
    
    def post_list(self, request, **kwargs):
        """
        Creates a new resource/object with the provided data.

        Calls ``obj_create`` with the provided data and returns a response
        with the new resource's location.

        If a new resource is created, return ``HttpCreated`` (201 Created).
        If ``Meta.always_return_data = True``, there will be a populated body
        of serialized data.
        """
        deserialized = self.deserialize(request, request.raw_post_data, format=request.META.get('CONTENT_TYPE', 'application/json'))
        deserialized = self.alter_deserialized_detail_data(request, deserialized)
        bundle = self.build_bundle(data=dict_strip_unicode_keys(deserialized), request=request)
        updated_bundle = self.obj_create(bundle, **self.remove_api_resource_names(kwargs))
        
        #Pass this bundle data through business logic layer
        if self._meta.__dict__.get('businessLogicLayer', None):
            self._meta.__dict__['businessLogicLayer'].process_bll(data = updated_bundle.data)
        
        location = self.get_resource_uri(updated_bundle)

        if not self._meta.always_return_data:
            response = json.dumps(getattr(updated_bundle, "response",None) )
            return http.HttpCreated(content = response,content_type="application/json",location=location)
        else:
            updated_bundle = self.full_dehydrate(updated_bundle)
            updated_bundle = self.alter_detail_data_to_serialize(request, updated_bundle)
            return self.create_response(request, updated_bundle, response_class=http.HttpCreated, location=location)

    
    def get_list(self, request, **kwargs):
        """
        Returns a serialized list of resources.

        Calls ``obj_get_list`` to provide the data, then handles that result
        set and serializes it.

        Should return a HttpResponse (200 OK).
        """
        # TODO: Uncached for now. Invalidation that works for everyone may be
        #       impossible.
        base_bundle = self.build_bundle(request=request)
        stats = self.obj_get_list(bundle=base_bundle, **self.remove_api_resource_names(kwargs))
        objects = stats
        count = 0
        if "objects" in stats and "count" in stats: 
            objects = stats.get("objects",[])
            count = stats.get("count",0)
        sorted_objects = self.apply_sorting(objects, options=request.GET)
        paginator = self._meta.paginator_class(request_data = request.GET, objects = sorted_objects,count=count,resource_uri=self.get_resource_uri(), limit=self._meta.limit, max_limit=self._meta.max_limit, collection_name=self._meta.collection_name)
        to_be_serialized = paginator.page()
        # Dehydrate the bundles in preparation for serialization.
        bundles = []
        
        for obj in to_be_serialized[self._meta.collection_name]:
            obj = AttributeStyleDict(obj) if type(obj) in (dict,) else obj
            bundle = self.build_bundle(obj=obj, request=request)
            bundles.append(self.full_dehydrate(bundle,for_list=True))

        to_be_serialized[self._meta.collection_name] = bundles
        to_be_serialized = self.alter_list_data_to_serialize(request, to_be_serialized)
        return self.create_response(request, to_be_serialized)
    
    def get_object_list(self, request, applicable_filters={}, **kwargs):
        pass
    
    @log_time
    def obj_get_list(self, bundle, **kwargs):
        filters = {}
        if hasattr(bundle.request, 'GET'):
            # Grab a mutable copy.
            filters = bundle.request.GET.copy()
        filters.update(kwargs)
        applicable_filters = filters
        try:
            db_params = self.get_pagination_parameters(bundle.request)
            if filters.get('fields'):
                required_fields = filters['fields'].split(',')
            else:
                required_fields = []
            stats = self.get_object_list(bundle.request, applicable_filters,required_fields, **db_params)
            auth_objs=self.authorized_read_list(stats.get("objects",[]), bundle)
            stats.update({"objects" : auth_objs})
            return stats
        except KeyError:
            raise BadRequest("Invalid resource lookup data provided (mismatched type).")

    def get_pagination_parameters(self,request):
        paginator = self._meta.paginator_class(request.GET, [], 20,resource_uri=self.get_resource_uri(), limit=self._meta.limit, max_limit=self._meta.max_limit, collection_name=self._meta.collection_name)
        return {"limit" : paginator.get_limit(), "skip" : paginator.get_offset()}
    
    @log_time
    def obj_get(self, bundle, **kwargs):
        try:
            pk = bundle.request.user.get("id", "") if kwargs["pk"].lower() == "me" else kwargs["pk"]
            data=self.get_obj(None,pk,**kwargs)
            if not data:
                raise ObjectDoesNotExist
            return AttributeStyleDict(data)
        except InvalidId:
            self.raise_id_not_found(kwargs['pk'])
        
    def obj_create(self, bundle, **kwargs):
        try:
            bundle.obj = self._meta.object_class()
#            self.authorized_create_detail(self.get_object_list(bundle.request), bundle)
            bundle = self.full_hydrate(bundle)
            data = bundle.data
            insert_datails = self.create_obj(None,data,safe=True)
            ins_id = insert_datails.get("_id","")
            if "data" in insert_datails:
                setattr(bundle, "response", insert_datails["data"])
            bundle.obj._id = ins_id
        except ValidationError as e:
            raise e
        except RequireFieldError:
            raise BadRequest("Required Field is Missing")
        except DuplicateKeyError as e:
            raise BadRequest("Cannot create a new instance for this resource ", e.message)
        return bundle
    
    def obj_update(self, bundle, skip_errors=False,request=None, **kwargs):
        self.authorized_update_detail(self.get_object_list(bundle.request), bundle)
        bundle = self.full_hydrate(bundle)
        try:
            data = bundle.data
            update_results = self.update_obj(None,kwargs["pk"],data,safe=True, fsync=True)
            obj_id = update_results.get("_id", "") 
            bundle.obj._id = obj_id
        except ObjectDoesNotExist:
            raise NotFound
        return bundle
    
    def obj_delete(self, bundle, **kwargs):
        """
        Deletes a single object.

        This needs to be implemented at the user level.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        self.authorized_delete_detail(self.get_object_list(bundle.request), bundle)
        obj_id = kwargs["pk"]
        self.delete_obj(None,doc_id=obj_id,**kwargs)

    def detail_uri_kwargs(self, bundle_or_obj):
        kwargs = {}
        '''get the uri for an object'''
        if isinstance(bundle_or_obj, Bundle):
            kwargs['pk'] = bundle_or_obj.obj._id
        else:
            kwargs['pk'] = bundle_or_obj.id
        return kwargs

    def rollback(self, bundles):
        pass
        
    def raise_id_not_found(self, identifier):
        raise BadRequest( "%s is not a valid id" % identifier)

    def dehydrate_uri(self, bundle):
        """
        For the automatically included ``uri`` field, dehydrate
        the URI for the given bundle.

        Returns empty string if no URI can be generated.
        """
        try:
            return self.get_resource_uri(bundle)
        except NotImplementedError:
            return ''
        except NoReverseMatch:
            return ''
