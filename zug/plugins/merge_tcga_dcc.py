import os
import imp
import requests
import logging
import yaml
import copy
import traceback
import uuid
import time 
import json
import random

from lxml import etree
from pprint import pprint
from zug import basePlugin
from zug.exceptions import IgnoreDocumentException, EndOfQueue

currentDir = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(name = "[{name}]".format(name = __name__))


class merge_tcga_dcc(basePlugin):

    """

    """
    
    def initialize(self, **kwargs):
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', '7474')
        self.url = 'http://{host}:{port}/db/data/cypher'.format(host=self.host, port=self.port)
        
        self.node_type = ":file"
        self.where = 'WHERE n.archive_name="{doc}"'

    def process(self, doc):

        result = self.submit(' '.join([
            'MATCH (n{node_type})',
            self.where.format(doc=doc).replace('.tar.gz', ''),
            'RETURN n',
            ]).format(node_type=self.node_type))
        pprint(result)
        return result

    def submit(self, cypher):
        data = {"query": cypher}
        print cypher
        r = requests.post(self.url, data=json.dumps(data))
        if r.status_code != 200: 
            logger.error("FAILURE: cypher query")
            return {}
        return r.json()
