import json

class Settings(object):
    def __init__(self,path,namespace,strict=True):
        # Load the json settings file provided.
        self.settings = json.loads(open(path).read(), strict=strict)[namespace]

    def __getitem__(self,key):
        # Returns the requested setting.
        return self.settings[key]

    def __setitem__(self,key,val):
        # Overrides the setting with a new value.
        # Note that this does not overwrite the file.
        self.settings[key] = val
