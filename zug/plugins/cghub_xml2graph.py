import os
import imp
import requests
import logging
import yaml
import copy
import traceback
import uuid
import sys

from lxml import etree
from pprint import pprint
from zug import basePlugin
from zug.exceptions import IgnoreDocumentException, EndOfQueue

currentDir = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(name = "[{name}]".format(name = __name__))

class cghub_xml2graph(basePlugin):

    """
    takes in an xml as a string and compiles a list of nodes and edges
    """

    [{
        'node': {
            'matches': {'key': 'value'}, # the key, values to match when making node
            'node_type': '',
            'body': {}, # these values will be written/overwritten
            'on_match': {}, # these values will only be written if the node ALREADY exists
            'on_create': {}, # these values will only be written if the node DOES NOT exist
        },
        'edges': {                    
            'matches': {'key': 'value' },  # the key, values to match when making edge
            'node_type': '',
            'edge_type': '',
        }
    },]
    
    def initialize(self, **kwargs):
        assert 'translate_path' in kwargs, "Please specify path to cghub_translate.yml"
        self.xml_root = None 
        self.namespaces = None
        self.graph = []

        with open(kwargs['translate_path']) as f: 
            self.translate = yaml.load(f)['result']

        self.edges = self.translate['edges']
        self.nodes = self.translate['nodes']
        self.properties = self.translate['properties']
    
    def process(self, doc):

        if doc is None: raise IgnoreDocumentException()

        try:
            graph = self.parse(copy.deepcopy(doc))
        except Exception, msg:
            logger.error(str(msg))
            logger.error(str(doc))
            traceback.print_exc()
            raise IgnoreDocumentException()

        return graph
        
    def parse(self, data, reset = True):

        xml_root = etree.fromstring(data).getroottree()
        results = xml_root.findall('Result')
        graph = []
        count = 0
        for result in results:
            count += 1
            graph += self.create_graph(result)
            if not count % 1000:
                logging.info("Completion: {perc} %".format(perc=count*100./len(results)))
                self.yieldDoc(copy.copy(graph))
                graph = []
                sys.stdout.flush()
        self.yieldDoc(graph)
            
    def create_graph(self, elem):
        graph = []
        for xml_node in elem.findall(self.nodes['locate']):
            node = {}
            self.add_properties(xml_node, self.nodes['properties'], node)
            self.add_properties(elem, self.properties, node)
            graph.append({
                'node': {
                    'matches': {self.nodes['match_to_key']: node[self.nodes['match_to_key']]},
                    'node_type': 'file',
                    'body': node,
                    'on_create': {'id': str(uuid.uuid4())},
                },
                'edges': self.get_edges(elem),
            })
            
        return graph

    def add_properties(self, elem, settings, node):
        for key, values in settings.items():
            for prop in elem.findall(values['locate']):
                if prop.text is None: continue
                node[key] = prop.text
        return node

    def get_edges(self, elem):
                
        edges = []
        for node_type, settings in self.edges.items():

            for edge in elem.findall(settings['locate']):
                value = edge.text
                if value is None: 
                    continue

                edges.append({ 
                    'matches': {settings['match_to_key']: value},
                    'node_type': node_type,
                    'edge_type': settings['edge_type'],
                })

        return edges
