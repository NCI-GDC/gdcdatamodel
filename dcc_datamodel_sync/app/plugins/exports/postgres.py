import os, imp

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('Export', basePath)

class Export(base.Export):

    def initialize(self, **kwargs):
        pass

    def export(self, doc, **kwargs):
        pass

    def close(self, **kwargs):
        pass
        
