import os, imp, requests, logging
from pprint import pprint

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('PipelinePlugin', basePath)

# stdout loader

class PipelinePlugin(base.PipelinePluginBase):

    """
    reverses doc
    """

    def __init__(self, **kwargs):
        self.docs = []
        pass

    def __iter__(self):
        for doc in self.docs:
            yield len(doc)

    def start(self, doc = None):
        self.docs.append(doc)

    def close(self, **kwargs):
        pass
        
