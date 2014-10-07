import os, imp, requests, logging
from pprint import pprint

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('PipelinePlugin', basePath)
logger     = logging.getLogger(name = "[{name}]".format(name = __name__))

class PipelinePlugin(base.PipelinePluginBase):

    """
    
    :input: Handles no input, reads files specified in :paths:
    :returns: Output docs are of type ``str``

    """

    def initialize(self, **kwargs):

        assert 'paths' in kwargs, "Please specify paths: setting in settings file"

        self.paths = kwargs['paths']
        self.splitLines = kwargs.get('splitLines', False)

    
    def __iter__(self):
        for doc in self.docs:
            if doc is None: continue
            yield doc

    def start(self):
        for path in self.paths:
            with open(path) as f:
                if not self.splitLines:
                    self.docs.append(f.read())
                else: 
                    self.docs += f.readlines()
        
