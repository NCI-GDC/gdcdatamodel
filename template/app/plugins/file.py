import os, imp, requests, logging
from pprint import pprint

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('PipelinePlugin', basePath)

# File pipeline stage

class PipelinePlugin(base.PipelinePluginBase):

    """
    File pipeline stage
    """

    def __init__(self, **kwargs):
        self.docs = []
        self.urls = []
        self.path = kwargs.get('path', None)

    def __iter__(self):
        for url in self.urls:
            local = url.split('/')[-1]
            logging.info("pulling file {url}".format(url = local))
            response = requests.get(url)
            doc = response.text
            yield doc

    def __call__(self, doc):
        logging.warn("File plugin was passed a document it doesn't know how to handle")

    def start(self, doc = None):
        with open(self.path) as f:
            self.urls = f.readlines()

    def close(self, **kwargs):
        pass
        
