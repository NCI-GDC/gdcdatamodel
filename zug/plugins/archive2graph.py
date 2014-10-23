import os
import imp
import requests
import logging
import yaml
import copy
import traceback

from lxml import etree
from pprint import pprint
from zug import basePlugin
from zug.exceptions import IgnoreDocumentException, EndOfQueue

currentDir = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(name = "[{name}]".format(name = __name__))

class archive2graph(basePlugin):

    """
    archive2graph
    takes in an xml as a string and compiles a list of nodes and edges

    edges = {
       source_id: {
          destination_id : (edge_type, destination_type),
       }
    } 

    """

    def initialize(self, **kwargs):
        self.props = {
            'archive_name': 'archive_name',
            'revision': 'revision',
            'date_added': 'date_added', 
        }
        
        # {edge_type: key}
        self.edges = {
            'batch': 'batch',
            'center': 
        }

    def process(self, doc):

        if doc is None: raise IgnoreDocumentException()

        try:
            parsed = self.parse(doc)
        except Exception, msg:
            logger.error(str(msg))
            logger.error(str(doc))
            traceback.print_exc()
            raise IgnoreDocumentException()

        return parsed
        

    def parse(self, doc):

        nodes = {}
        edges = {}
        ret = {'edges': edges, 'nodes': nodes}

        id = doc['archive_name']
        nodes[id] = {}



        nodes[id][id] = doc['archive_name']
        nodes[id]['_type'] = doc['_type']
        nodes[id]['batch'] = doc['batch']
        nodes[id]['disease_code'] = doc['']
        nodes[id][''] = doc['']
        nodes[id][''] = doc['']
        nodes[id][''] = doc['']
        nodes[id][''] = doc['']
        nodes[id][''] = doc['']
        nodes[id][''] = doc['']
        nodes[id][''] = doc['']
        
        return doc
