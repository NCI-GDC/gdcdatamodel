import os
import imp
import requests
import logging
import yaml

from lxml import etree
from pprint import pprint
from zug import basePlugin

currentDir = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(name = "[{name}]".format(name = __name__))


class xml2graph(basePlugin):

    """
    xml2graph
    takes in an xml as a string and compiles a list of nodes and edges

    edges = {
       source_id: {
          destination_id : (edge_type, destination_type),
       }
    } 

    """

    def initialize(self, **kwargs):
        assert 'translate_path' in kwargs, "Please specify path to translate.yml"
        assert 'data_type'      in kwargs, "Please specify data_type (i.e. biospecimen)"

        self.xml_root = None
        self.namespaces = None
        self.nodes = {}
        self.edges = {}
        self.doc = {'edges': self.edges, 'nodes': self.nodes}

        with open(kwargs['translate_path']) as f: 
            self.translate = yaml.load(f)

        self.node_types = self.translate[kwargs['data_type']]
    
    def next(self, doc):
        return self.parse(doc)

    def parse(self, data, reset = True):

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
                    logging.warn('Node [{ntype}] does not have one id: {ids}'.format(ids=node_id, ntype=node_type))
                    return self.doc

                node['id'] = node_id[0]

                self.nodes[node['id']] = node
                self.load_properties(xml_node, node_settings, node)
                self.get_edges(xml_node, node, edges)

        return self.doc

    def get_edges(self, elem, node, edge_types):

        for edge_type, edge_settings in edge_types.iteritems():

            endpoints = elem.xpath(edge_settings['locate'], namespaces=self.namespaces)
            src_id = node['id']
            if src_id not in self.edges: self.edges[src_id] = {}

            for dst_id in endpoints:
                edge = (edge_type, edge_settings['type'])
                self.edges[src_id][dst_id] = edge
    

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
