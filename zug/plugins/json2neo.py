import os, imp, requests, logging
from pprint import pprint
from py2neo import neo4j, node

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('PipelinePlugin', basePath)
logger     = logging.getLogger(name = "[{name}]".format(name = __name__))

class PipelinePlugin(base.PipelinePluginBase):

    """
    
    """
    
    def initialize(self, **kwargs):

        host = kwargs.get('host', 'localhost')
        port = kwargs.get('port', 7474)
        user = kwargs.get('user', None)
        password = kwargs.get('password', None)

        self.db = neo4j.GraphDatabaseService()

    def process(self, doc):

        assert 'docs' in doc, "Stage {name} was passed a doc without field 'docs'".format(name=__name__)
        assert 'edge_types' in doc, "Stage {name} was passed a doc without field 'edge_types'".format(name=__name__)

        for entry in doc['docs']:
            self.db.create(node(entry))

        return doc
        
