import yaml
import logging
import json
import argparse
from lxml import etree


class BiospecimenParser:

    def __init__(self, node_types):

        self.node_types = node_types
        self.xml_root = None
        self.namespaces = None
        self.nodes = {}
        self.edges = {}

    def load_properties(self, elem, node_settings, properties):
        if 'properties' not in node_settings:
            return None

        property_xpath = node_settings['properties']
        
        property_nodes = elem.xpath(property_xpath, 
                                    namespaces=self.namespaces)
            
        for property_node in property_nodes:
            tag_nons = property_node.xpath('local-name()')
            
            #Fix up property names if requested
            if 'property_map' in node_settings:
                if tag_nons in node_settings['property_name_map']:
                    tag_nons = node_settings['property_name_map'][tag_nons]

            #Could make into list, for now it's an error
            if tag_nons in properties:
                logging.error('Already seen property: %s' % tag_nons)
                raise

            if property_node.text is not None:
                properties[tag_nons] = property_node.text.strip()
            else:
                properties[tag_nons] = property_node.text

    def get_edges(self, elem, node, edge_types):
        for edge_type, edge_settings in edge_types.iteritems():
            endpoints = elem.xpath(edge_settings['locate'],
                                   namespaces=self.namespaces)

            if edge_type not in self.edges:
                self.edges[edge_type] = []

            for endpoint_id in endpoints:
                edge = ((node['_type'], node['id'], ),(edge_settings['type'], endpoint_id))
                self.edges[edge_type].append(edge)
                
    def parse_biospecimen(self, filename = None, data = None):

        if filename and not data:
            self.xml_root = etree.parse(filename)
        elif data and not filename:
            self.xml_root = etree.fromstring(data).getroottree()
        else:
            raise Exception('Please specify filename OR data string')

        self.namespaces = self.xml_root.getroot().nsmap

        for node_type in self.node_types:
            node_settings = self.node_types[node_type]
            xml_nodes = self.xml_root.xpath(node_settings['locate'], 
                                            namespaces=self.namespaces)

            for xml_node in xml_nodes:
                node = {'_type' : node_settings['_type']}
                node_id = xml_node.xpath(node_settings['id'], 
                                         namespaces=self.namespaces)

                if len(node_id) != 1:
                    logging.error('Node does not have one id: %s' % node_id)
                    raise
                else:
                    node['id'] = node_id[0]

                self.load_properties(xml_node, node_settings, node)

                self.nodes[node['id']] = node

                if 'edges' in node_settings:
                    self.get_edges(xml_node, node, node_settings['edges'])

    def print_graph(self):
        for node in self.nodes:
            logging.info(json.dumps(self.nodes[node], indent=4, sort_keys=True))

        for edge_type, edges in self.edges.iteritems():
            logging.info("%s:" % edge_type)
            for edge in edges:
                logging.info(edge)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Testing BCR XML Parsing')

    parser.add_argument('-l','--loglevel', default=logging.INFO, help='Set logging level.')
    parser.add_argument('filename', type=str, help='Biospecimen filename')
    args = parser.parse_args()

    logging.basicConfig(
        level = logging.getLevelName(args.loglevel),
        format = '%(asctime)s %(name)-6s %(levelname)-4s %(message)s',
    )

    yaml_path = 'translate.yaml'
    translate = yaml.load(open('translate.yml'))
    bp = BiospecimenParser(translate['biospecimen'])
    bp.parse_biospecimen(args.filename)
    bp.print_graph()
