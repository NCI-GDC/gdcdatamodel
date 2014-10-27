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

class xml2graph(basePlugin):

    """
    xml2graph
    takes in an xml as a string and compiles a list of nodes and edges

    [{
        'edges': {                    
            'matches': {'key': value }  # the key, values to match when making edge
            'node_type': '',
            'edge_type': '',
        'node': {
            'matches': {'key': value}, # the key, values to match when making node
            'node_type': '',
            'body': {}
        }
    },]

    """
    
    def initialize(self, **kwargs):
        assert 'translate_path' in kwargs, "Please specify path to translate.yml"
        assert 'data_type'      in kwargs, "Please specify data_type (i.e. biospecimen)"

        self.xml_root = None 
        self.namespaces = None
        self.graph = []

        with open(kwargs['translate_path']) as f: 
            self.translate = yaml.load(f)

        self.node_types = self.translate[kwargs['data_type']]
    
    def process(self, doc):

        if doc is None: raise IgnoreDocumentException()

        try:
            graph = self.parse(copy.deepcopy(doc))
        except Exception, msg:
            logger.error(str(msg))
            logger.debug(str(doc))
            traceback.print_exc()
            raise IgnoreDocumentException()

        return graph
        
    def parse(self, data, reset = True):

        graph = []
        # Base xml
        self.xml_root = etree.fromstring(data).getroottree()
        self.namespaces = self.xml_root.getroot().nsmap

        for node_type in self.node_types:

            node_settings = self.node_types[node_type]
            edges = node_settings.get('edges', None)
            xml_nodes = self.xml_root.xpath(node_settings['locate'], namespaces=self.namespaces)

            for xml_node in xml_nodes:
                node = {'_type' : node_settings['_type']}
                node_id = xml_node.xpath(node_settings['id'], namespaces=self.namespaces)

                if len(node_id) != 1:
                    logger.warn('Node [{ntype}] does not have one id: {ids}'.format(ids=node_id, ntype=node_type))
                    return 

                node['id'] = node_id[0]

                self.load_properties(xml_node, node_settings, node)
                edge = self.get_edges(xml_node, node, edges)

                graph.append({
                    'edges': edge,
                    'node': {
                        'node_type': node['_type'],
                        'matches': {'id': node_id[0]},
                        'body': node
                    }
                })

        return graph

    def get_edges(self, elem, node, edge_types):

        edges = []
        for edge_type, edge_settings in edge_types.iteritems():

            endpoints = elem.xpath(edge_settings['locate'], namespaces=self.namespaces)
            src_id = node['id']

            for dst_id in endpoints:
                edges.append({
                    'edge_type': edge_type,
                    'node_type': edge_settings['type'],
                    'matches': {
                        'id': dst_id
                    }
                })

        return edges

    def load_properties(self, elem, node_settings, properties):

        if 'properties' not in node_settings:
            logger.warn("No properties found")
            return None

        property_xpath = node_settings['properties']
        
        property_nodes = elem.xpath(property_xpath, namespaces=self.namespaces)
            
        for property_node in property_nodes:
            tag_nons = property_node.xpath('local-name()')
            
            #Fix up property names if requested
            if 'property_map' in node_settings:
                if tag_nons in node_settings['property_name_map']:
                    tag_nons = node_settings['property_name_map'][tag_nons]

            #Could make into list, for now it's an error
            if tag_nons in properties:
                logging.error('Duplicate property: %s' % tag_nons)
                raise

            if property_node.text is not None:
                properties[tag_nons] = property_node.text.strip()
            else:
                properties[tag_nons] = property_node.text
