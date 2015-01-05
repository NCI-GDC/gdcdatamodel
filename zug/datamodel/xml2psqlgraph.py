import yaml
import json
import logging
import psqlgraph
from lxml import etree

logger = logging.getLogger(name="[{name}]".format(name=__name__))


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


class xml2psqlgraph(object):

    """
    """

    def __init__(self, translate_path, host, user,
                 password, database, node_validator=None,
                 edge_validator=None, **kwargs):
        """

        :param str translate_path:
            path to translation mapping yaml file

        """

        self.graph = []
        self.namespaces = None
        with open(translate_path) as f:
            self.translate = json.loads(json.dumps(yaml.load(f)),
                                        object_hook=AttrDict)
        self.graph = psqlgraph.PsqlGraphDriver(
            host=host, user=user, password=password, database=database)

    def xpath(self, path, root=None, single=False, nullable=True,
              expected=True, text=True, label=''):

        if root is None:
            root = self.xml_root
        try:
            result = root.xpath(path, namespaces=self.namespaces)
        except etree.XPathEvalError:
            result = []
        except:
            raise
        rlen = len(result)

        if rlen < 1 and expected:
            raise Exception('{}: Unable to find {}'.format(label, path))

        elif rlen > 1 and single:
            logging.error(result)
            raise Exception('{}: Expected 1 result for {}, found {}'.format(
                label, path, result))

        if text:
            result = [r.text for r in result]
            if not nullable and None in result:
                raise Exception('{}: Null result for {}'.format(label, result))

        if single:
            result = result[0]

        return result

    def xml2psqlgraph(self, data):
        if not data:
            return None
        self.xml_root = etree.fromstring(data).getroottree()
        self.namespaces = self.xml_root.getroot().nsmap
        for node_type, params in self.translate.items():
            self.parse_node(node_type, params)

    def parse_node(self, node_type, params):
        roots = self.get_node_roots(node_type, params)
        for root in roots:
            node_id = self.get_node_id(root, node_type, params)
            nprops = self.get_node_properties(root, node_type, params, node_id)
            edges = self.get_node_edges(root, node_type, params, node_id)
            self.insert_node(node_id, node_type, nprops)
            for dst_id, edge in edges.items():
                dst_label, edge_label = edge
                self.insert_edge(node_id, dst_id, dst_label, edge_label)

    def insert_node(self, node_id, label, properties):
        self.graph.node_merge(
            node_id=node_id,
            label=label,
            properties=properties
        )

    def insert_edge(self, src_id, dst_id, dst_label, edge_label,
                    properties={}):
        self.graph.node_merge(node_id=dst_id, label=dst_label)
        self.graph.edge_merge(
            src_id=src_id,
            dst_id=dst_id,
            label=edge_label,
            properties=properties
        )

    def get_node_roots(self, node_type, params):
        if not params.root:
            logging.warn('No root xpath for {}'.format(node_type))
            return
        xml_node = self.xpath(params.root, text=False, label='get_node_roots')
        return xml_node

    def get_node_id(self, root, node_type, params):
        if not params.id:
            return None
        node_id = self.xpath(params.id, root, single=True, label=node_type)
        return node_id

    def get_node_properties(self, root, node_type, params, node_id=''):
        props = {}
        for prop, path in params.properties.items():
            if not path:
                continue
            result = self.xpath(
                path, root, single=True, text=True,
                label='{}: {}'.format(node_type, node_id))
            props[prop] = result
        return props

    def get_node_edges(self, root, node_type, params, node_id=''):
        edges = {}
        for edge_type, paths in params.edges.items():
            for dst_label, path in paths.items():
                if not path:
                    continue
                results = self.xpath(
                    path, root, expected=False, text=True,
                    label='{}: {}'.format(node_type, node_id))
                for result in results:
                    edges[result] = (dst_label, edge_type)
        return edges
