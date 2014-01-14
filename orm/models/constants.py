'''
Created on 08-Oct-2013

@author: Rahul
'''
DJANGO_TO_MONGO_MAPPER = {
    "lt": "$lt", "lte": "$lte", "gt": "$gt", "gte": "$gte",
    "ne": "$ne", "exists": "$exists", "in": "$in", "notin": "$nin", "like": "$regex"}
LOOKUP_SEP = '__'
