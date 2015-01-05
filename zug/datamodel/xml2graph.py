import yaml
import logging
import argparse
import psqlgraph
from lxml import etree
from pprint import pprint

logger = logging.getLogger(name="[{name}]".format(name=__name__))


class xml2graph(object):

    """
    xml2graph
    takes in an xml as a string and inserts it into psqlgraph
    """

    def __init__(self, translate_path, data_type, **kwargs):
        """

        :param str translate_path:
            path to translation mapping yaml file
        :param str data_type:
            data type used to filter against, must be a root value in
            translate_path yaml mapping

        """

        self.xml_root = None
        self.namespaces = None
        self.graph = []

        with open(translate_path) as f:
            self.translate = yaml.load(f)

        self.node_types = self.translate[data_type]

    def process(self, doc):

        if doc is None:
            return None

        try:
            return self.parse(doc)
        except Exception, msg:
            logger.error(str(msg))
            logger.error(str(doc))
            raise

    def parse(self, data):

        graph = []
        # Base xml
        self.xml_root = etree.fromstring(data).getroottree()
        self.namespaces = self.xml_root.getroot().nsmap

        for node_type in self.node_types:

            node_settings = self.node_types[node_type]
            edges = node_settings.get('edges', None)
            xml_nodes = self.xml_root.xpath(node_settings['locate'],
                                            namespaces=self.namespaces)

            for xml_node in xml_nodes:
                node_id = xml_node.xpath(
                    node_settings['id'], namespaces=self.namespaces)

                node = {}
                if len(node_id) != 1:
                    raise Exception('Node [{ntype}] does not have one id: '
                                    '{}'.format(ntype=node_type))

                self.load_properties(xml_node, node_settings, node)
                edge = self.get_edges(xml_node, node, edges)

                graph.append({
                    'edges': edge,
                    'node': {
                        'label': node_settings['_type'],
                        'id': node_id[0],
                        'body': node_id
                    }
                })

        return graph

    def get_edges(self, elem, node, edge_types):

        edges = []
        for edge_type, edge_settings in edge_types.iteritems():

            endpoints = elem.xpath(edge_settings['locate'],
                                   namespaces=self.namespaces)

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

            # Fix up property names if requested
            if 'property_map' in node_settings:
                if tag_nons in node_settings['property_name_map']:
                    tag_nons = node_settings['property_name_map'][tag_nons]

            # Could make into list, for now it's an error
            if tag_nons in properties:
                logging.error('Duplicate property: %s' % tag_nons)
                raise

            if property_node.text is not None:
                properties[tag_nons] = property_node.text.strip()
            else:
                properties[tag_nons] = property_node.text


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("xml_path",
                        help='the path to convert to graphical representation')
    parser.add_argument("mapping_path",
                        help='the path to the yaml mapping file')
    parser.add_argument("data_type", help='the data_type to filter')

    args = parser.parse_args()
    parser = xml2graph(args.mapping_path, args.data_type)
    with open(args.xml_path, 'r') as f:
        pprint(parser.parse(f.read()))
