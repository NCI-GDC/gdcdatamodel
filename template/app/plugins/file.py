import os, imp, requests, logging
from pprint import pprint

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('PipelinePlugin', basePath)
logger = logging.getLogger(name = "[{name}]".format(name = __name__))

class PipelinePlugin(base.PipelinePluginBase):

    """
    File pipeline stage
    """

    def initialize(self, **kwargs):
        self.urls = []
        self.path = kwargs.get('path', None)

    def __iter__(self):
        for url in self.urls:
            
            # Generate a document
            local = url.split('/')[-1]
            logger.info("pulling file {url}".format(url = local.strip()))
            response = requests.get(url)
            doc = response.text

            # Pass state to the next stage
            self.state['url'] = url.strip()

            yield doc

    def start(self):
        with open(self.path) as f:
            self.urls = f.readlines()

    def close(self, **kwargs):
        pass
        
