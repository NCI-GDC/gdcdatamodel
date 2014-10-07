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

    def __iter__(self):

        for doc in self.docs:
            for url in doc:
                logger.info("downloading file {doc}".format(doc = url))
                response = requests.get(url)
                doc = response.text
    
                # Pass state to the next stage
                self.state['doc'] = doc.strip()
    
                yield doc
    
