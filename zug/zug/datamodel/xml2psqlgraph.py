import yaml
import json
import logging
import psqlgraph
import re
from lxml import etree

logger = logging.getLogger(name="[{name}]".format(name=__name__))


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


class xml2psqlgraph(object):

    """
    """

    def __init__(self, translate_path, data_type, host, user,
                 password, database, node_validator=None,
                 edge_validator=None, **kwargs):
        """

        :param str translate_path:
            path to translation mapping yaml file
        :param str data_type:
            data type used to filter against, must be a root value in
            translate_path yaml mapping

        """

        self.graph = []
        self.namespaces = None
        with open(translate_path) as f:
            self.translate = json.loads(json.dumps(yaml.load(f)),
                                        object_hook=AttrDict)
        self.graph = psqlgraph.PsqlGraphDriver(
            host=host, user=user, password=password, database=database)

    def xpath(self, path, root=None, single=False, nullable=True):
        if root is None:
            root = self.xml_root
        try:
            result = root.xpath(path, namespaces=self.namespaces)
        except etree.XPathEvalError:
            result = []
        except:
            raise

        if len(result) < 1:
            if not nullable:
                raise Exception('Unable to finde {}'.format(path))

        if single and len(result) > 1:
            logging.error(result)
            raise Exception('Expected 1 result for {}, found {}'.format(
                path, result))

        if single:
            if len(result) > 0:
                return result[0]
            else:
                return None

        return result

    def add_to_graph(self, data):
        if not data:
            return None
        self.xml_root = etree.fromstring(data).getroottree()
        self.namespaces = self.xml_root.getroot().nsmap
        for node_type, params in self.translate.items():
            self.insert_node(node_type, params)

    def insert_node(self, node_type, params):
        roots = self.get_node_roots(node_type, params)
        for root in roots:
            props = self.get_node_properties(root, node_type, params)
            node_id = self.get_node_id(root, node_type, params)
            edges = self.get_node_edges(root, node_type, params)

            print '\n{}: {}'.format(node_type, node_id)
            print 'props: ', props
            print 'edges: ', edges

    def get_node_roots(self, node_type, params):
        if not params.root:
            logging.warn('No root xpath for {}'.format(node_type))
            return
        xml_node = self.xpath(params.root)
        return xml_node

    def get_node_id(self, root, node_type, params):
        path = params.id
        if not path:
            return None
        node_id = self.xpath(path, root, single=True, nullable=False)
        return node_id

    def get_node_properties(self, root, node_type, params):
        props = {}
        for prop, path in params.properties.items():
            if not path:
                continue
            result = self.xpath(path+'/text()', root, single=True)
            if result:
                props[prop] = result
        return props

    def get_node_edges(self, root, node_type, params):
        edges = {}
        for edge, path in params.edges.items():
            if not path:
                continue
            results = self.xpath(path+'/text()', root)
            if results:
                edges[edge] = results
        return edges

    # def add_node_type(self, node_type):
    #     node_settings = self.node_types[node_type]
    #     edge_types = node_settings.get('edges', None)
    #     try:
    #         xml_nodes = self.xml_root.xpath(
    #             node_settings['locate'], namespaces=self.namespaces)
    #     except etree.XPathEvalError:
    #         if self.xml_root.getroot().tag not in namespace_exceptions:
    #             logging.error(self.xml_root.getroot().tag)
    #         return
    #     except Exception, msg:
    #         logging.error(etree.tostring(self.xml_root, pretty_print=True))
    #         logging.error('Unable to get xml_nodes: '+str(msg))
    #         raise

    #     for xml_node in xml_nodes:
    #         try:
    #             node_id = self.add_node(xml_node, node_settings, node_type)
    #             if node_id:
    #                 self.add_edge_types(node_id, xml_node, edge_types)
    #         except Exception, msg:
    #             logging.error('Unable to add node and edges: '+str(msg))
    #             raise

    # def add_node(self, xml_node, node_settings, node_type):

    #     node_ids = xml_node.xpath(
    #         node_settings['id'],
    #         namespaces=self.namespaces
    #     )

    #     if len(node_ids) != 1:
    #         if node_type == 'protocol':
    #             return None
    #         print(etree.tostring(xml_node, pretty_print=True))
    #         raise Exception('ID count {} != 0 for {}'.format(
    #             node_ids, node_type))

    #     node_id = node_ids[0]
    #     properties = {}
    #     self.load_properties(xml_node, node_settings, properties)

    #     try:
    #         self.psqlgraphDriver.node_merge(
    #             node_id=node_id,
    #             label=node_settings['_type'],
    #             properties=properties,
    #         )
    #     except:
    #         logging.error(node_id)
    #         logging.error(properties)
    #         logging.error('Unable to insert node')
    #         raise

    #     return node_id

    # def add_edge_types(self, src_id, elem, edge_types):
    #     for edge_label, edge_settings in edge_types.iteritems():
    #         dst_ids = elem.xpath(
    #             edge_settings['locate'],
    #             namespaces=self.namespaces
    #         )
    #         dst_label = edge_settings['type']
    #         self.add_edges(src_id, dst_ids, dst_label, edge_label)

    # def add_edges(self, src_id, dst_ids, dst_label, edge_label):
    #     for dst_id in dst_ids:
    #         self.psqlgraphDriver.node_merge(
    #             node_id=dst_id, label=dst_label)
    #         self.psqlgraphDriver.edge_merge(
    #             src_id=src_id, dst_id=dst_id, label=edge_label)

    # def load_properties(self, elem, node_settings, properties):

    #     if 'properties' not in node_settings:
    #         logger.warn("No properties found")
    #         return None

    #     property_xpath = node_settings['properties']

    #     property_nodes = elem.xpath(property_xpath, namespaces=self.namespaces)

    #     for property_node in property_nodes:
    #         tag_nons = property_node.xpath('local-name()')

    #         # Fix up property names if requested
    #         if 'property_map' in node_settings:
    #             if tag_nons in node_settings['property_name_map']:
    #                 tag_nons = node_settings['property_name_map'][tag_nons]

    #         # Could make into list, for now it's an error
    #         if tag_nons in properties:
    #             logging.error('Duplicate property: %s' % tag_nons)
    #             raise

    #         if property_node.text is not None:
    #             properties[tag_nons] = property_node.text.strip()
    #         else:
    #             properties[tag_nons] = property_node.text
