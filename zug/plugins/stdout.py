import os, imp, requests, logging
from pprint import pprint

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('PipelinePlugin', basePath)
logger = logging.getLogger(name = "[{name}]".format(name = __name__))

class PipelinePlugin(base.PipelinePluginBase):

    """
    Prints to stdout
    """

    def initialize(self, **kwargs):
        self.pprint = kwargs.get('pprint', False)

    def __iter__(self):
        for doc in self.docs:

            if self.pprint: 
                pprint(doc)
            else:
                print doc

            yield doc

