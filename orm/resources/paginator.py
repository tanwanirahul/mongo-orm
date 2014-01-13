'''
Created on 09-Oct-2013

@author: rahul
'''
from tastypie.paginator import Paginator

class Paginator(Paginator):
    def __init__(self, request_data, objects, count,resource_uri=None, limit=None, offset=0, max_limit=1000, collection_name='objects'):
        self.request_data = request_data
        self.objects = objects
        self.limit = limit
        self.count = count
        self.max_limit = max_limit
        self.offset = offset
        self.resource_uri = resource_uri
        self.collection_name = collection_name
    
    def get_slice(self, limit, offset):
        """
        Slices the result set to the specified ``limit`` & ``offset``.
        """
        return self.objects

    def get_count(self):
        """
        Returns a count of the total number of objects seen.
        """
        return self.count
