import os, imp
from pprint import pprint

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('Export', basePath)

class Export(base.Export):

    def __init__(self, **kwargs):
        pass

    def export(self, doc, **kwargs):
        pprint(doc)

    def close(self, **kwargs):
        pass
        
