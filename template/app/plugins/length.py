import os, imp, requests, logging
from pprint import pprint

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('PipelinePlugin', basePath)
logger = logging.getLogger(name = "[{name}]".format(name = __name__))

# stdout loader

class PipelinePlugin(base.PipelinePluginBase):

    """
    reverses doc
    """

    def __iter__(self):

        url = self.state.get('url', False)
        if url: logger.info("doc from {url}".format(url = url))

        for doc in self.docs:
            yield len(doc)
        
