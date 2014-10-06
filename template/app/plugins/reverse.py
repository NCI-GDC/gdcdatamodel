import os, imp, requests, logging
from pprint import pprint

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('PipelinePlugin', basePath)

class PipelinePlugin(base.PipelinePluginBase):

    """
    reverses doc
    """

    def __iter__(self):
        for doc in self.docs:
            yield doc[::-1]

    def start(self, doc = None):
        self.docs = [doc]
        
