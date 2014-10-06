import os, imp, requests, logging
from pprint import pprint

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('PipelinePlugin', basePath)

class PipelinePlugin(base.PipelinePluginBase):

    """
    Prints to stdout
    """

    def __iter__(self):
        for doc in self.docs:
            pprint(doc)
            yield doc

    def start(self, doc = None):
        self.docs = [doc]
        
