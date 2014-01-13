class AttributeStyleDict(dict):
    '''Convenience class to access a dictionary using attributes
        i.e. obj.somekey is the same as obj['somekey'] 
    '''
    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            return None
