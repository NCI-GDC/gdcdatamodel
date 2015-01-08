import yaml
import json
import datetime
import logging
import psqlgraph
import pprint
from psqlgraph.edge import PsqlEdge
from psqlgraph.node import PsqlNode
from lxml import etree

logger = logging.getLogger(name="[{name}]".format(name=__name__))

possible_true_values = [
    'true',
    'yes',
]

possible_false_values = [
    'false',
    'no',
]


def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return int(delta.total_seconds())


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def to_bool(val):
    if val.lower() in possible_true_values:
        return True
    elif val.lower() in possible_false_values:
        return False
    else:
        raise ValueError("Cannot convert {} to boolean".format(val))


class xml2psqlgraph(object):

    """
    """

    def __init__(self, translate_path, host, user,
                 password, database, node_validator=None,
                 edge_validator=None, ignore_missing_properties=False,
                 **kwargs):
        """

        :param str translate_path:
            path to translation mapping yaml file

        """

        self.graph = []
        self.namespaces = None
        self.ignore_missing_properties = ignore_missing_properties
        with open(translate_path) as f:
            self.translate = json.loads(json.dumps(yaml.load(f)),
                                        object_hook=AttrDict)
        self.graph = psqlgraph.PsqlGraphDriver(
            host=host, user=user, password=password, database=database)
        if node_validator:
            self.graph.node_validator = node_validator
        if edge_validator:
            self.graph.edge_validator = edge_validator
        self.nodes, self.edges = {}, {}

    def xpath(self, path, root=None, single=False, nullable=True,
              expected=True, text=True, label=''):
        """Wrapper to perform the xpath queries on the xml

        :param str path: The xpath location path
        :param root: the lxml element to perform query on
        :param bool single:
            Raise exception if the expected result is not singular as
            expected
        :param bool nullable:
            Raise exception if the result is null
        :param bool expected:
            Raise exception if the expected result does not exist
        :param bool text: whether the return value is the .text str value
        :param str label: label for logging

        """

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

        if rlen < 1 and not expected and single:
            return None

        if rlen < 1 and not expected and not single:
            return []

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

    def export(self):
        self.export_nodes()
        self.export_edges()

    def export_nodes(self):
        with self.graph.session_scope() as session:
            for node_id, n in self.nodes.iteritems():
                try:
                    self.graph.node_merge(
                        node_id=n.node_id, properties=n.properties,
                        label=n.label, session=session)
                except:
                    logging.error('Unable to add node')
                    print n
                    pprint.pprint(n.properties)
                    raise

    def export_edges(self):
        with self.graph.session_scope() as session:
            for edge_id, e in self.edges.iteritems():
                existing = list(self.graph.edge_lookup(
                    src_id=e.src_id, dst_id=e.dst_id, label=e.label,
                    session=session).all())
                if not len(existing):
                    try:
                        self.graph.edge_insert(e, session=session)
                    except:
                        logging.error('Unable to add edge')
                        print e
                        raise

    def xml2psqlgraph(self, data):
        """Main function that takes xml string and converts it to a graph to
        insert into psqlgraph

        :param str data: xml string to convert and insert

        """

        if not data:
            return None
        self.xml_root = etree.fromstring(data).getroottree()
        self.namespaces = self.xml_root.getroot().nsmap
        for node_type, params in self.translate.items():
            self.parse_node(node_type, params)

    def parse_node(self, node_type, params):
        """Convert a subsection of the xml that will be treated as a node

        :param str node_type: the type of node to be used as a label
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file

        """
        roots = self.get_node_roots(node_type, params)
        for root in roots:
            node_id = self.get_node_id(root, node_type, params)
            nprops = self.get_node_properties(
                root, node_type, params, node_id)
            n_date_props = self.get_node_datetime_properties(
                root, node_type, params, node_id)
            nprops.update(n_date_props)
            edges = self.get_node_edges(root, node_type, params, node_id)
            self.insert_node(node_id, node_type, nprops)
            for dst_id, edge in edges.items():
                dst_label, edge_label = edge
                self.insert_edge(node_id, dst_id, dst_label, edge_label)

    def insert_node(self, node_id, label, properties):
        """Adds a node to the graph

        """
        if node_id in self.nodes:
            self.nodes[node_id].merge(properties=properties)
        else:
            self.nodes[node_id] = PsqlNode(
                node_id=node_id,
                label=label,
                properties=properties
            )

    def insert_edge(self, src_id, dst_id, dst_label, edge_label,
                    properties={}):
        """Adds an edge to the graph

        """

        edge_id = "{}:{}:{}".format(src_id, dst_id, edge_label)
        self.insert_node(dst_id, dst_label, {})
        if edge_id in self.edges:
            self.edges[edge_id].merge(properties=properties)
        else:
            self.edges[edge_id] = PsqlEdge(
                src_id=src_id,
                dst_id=dst_id,
                label=edge_label,
                properties=properties
            )

    def get_node_roots(self, node_type, params):
        """returns a list of xml node root elements for a given node_type

        :param str node_type:
            the node type to be used as a label in psqlgraph
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file

        """
        if not params.root:
            logging.warn('No root xpath for {}'.format(node_type))
            return
        xml_node = self.xpath(
            params.root, expected=False, text=False, label='get_node_roots')
        return xml_node

    def get_node_id(self, root, node_type, params):
        """lookup the id for the node

        :param root: the lxml root element to treat as a node
        :param str node_type:
            the node type to be used as a label in psqlgraph
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file

        """
        if not params.id:
            return None
        node_id = self.xpath(params.id, root, single=True, label=node_type)
        return node_id

    def get_node_properties(self, root, node_type, params, node_id=''):
        """for each parameter in the setting file, try and look it up, and add
        it to the node properties

        :param root: the lxml root element to treat as a node
        :param str node_type:
            the node type to be used as a label in psqlgraph
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file
        :param str node_id: used for logging

        """

        if 'properties' not in params or not params.properties:
            return {}

        types = {'int': int, 'float': float, 'str': str, 'long': long}
        props = {}
        for prop, args in params.properties.items():
            path, _type = args['path'], args['type']
            if not path:
                continue
            result = self.xpath(
                path, root, single=True, text=True,
                expected=(not self.ignore_missing_properties),
                label='{}: {}'.format(node_type, node_id))
            if _type == 'bool':
                result = to_bool(result)
            else:
                result = types[_type](result) if result else result
            props[prop] = result
        return props

    def get_node_datetime_properties(
            self, root, node_type, params, node_id=''):
        """for datetime each parameter in the setting file, try and look it
        up, and add it to the node properties

        :param root: the lxml root element to treat as a node
        :param str node_type:
            the node type to be used as a label in psqlgraph
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file
        :param str node_id: used for logging

        """

        if 'datetime_properties' not in params or \
           not params.datetime_properties:
            return {}
        props = {}

        # Loop over all given datetime properties
        for name, timespans in params.datetime_properties.items():
            times = {'year': 0, 'month': 0, 'day': 0}

            # Parse the year, month, day
            for span in times:
                if span in timespans:
                    temp = self.xpath(
                        timespans[span], root, single=True, text=True,
                        label='{}: {}'.format(node_type, node_id))
                    times[span] = 0 if temp is None else int(temp)

            if not times['year']:
                props[name] = 0
            else:
                props[name] = unix_time(datetime.datetime(
                    times['year'], times['month'], times['day']))

        return props

    def get_node_edges(self, *args, **kwargs):
        """for each edge type in the settings file, lookup the possible edges

        :param root: the lxml root element to treat as a node
        :param str node_type:
            the node type to be used as a label in psqlgraph
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file
        :param str node_id: used for logging

        :returns: a dict of edges
        """

        edges = {}
        edges.update(self.get_node_edges_by_properties(*args, **kwargs))
        edges.update(self.get_node_edges_by_id(*args, **kwargs))
        return edges

    def get_node_edges_by_id(self, root, node_type, params, node_id=''):
        """for each edge type in the settings file, lookup the possible edges

        :param root: the lxml root element to treat as a node
        :param str node_type:
            the node type to be used as a label in psqlgraph
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file
        :param str node_id: used for logging

        :returns: a list of edges
        """

        edges = {}
        if 'edges' not in params or not params.edges:
            return edges
        for edge_type, paths in params.edges.items():
            for dst_label, path in paths.items():
                results = self.xpath(
                    path, root, expected=False, text=True,
                    label='{}: {}'.format(node_type, node_id))
                for result in results:
                    edges[result] = (dst_label, edge_type)
        return edges

    def get_node_edges_by_properties(self, root, node_type, params,
                                     node_id=''):
        """for each edge type in the settings file, lookup the possible edges

        :param root: the lxml root element to treat as a node
        :param str node_type:
            the node type to be used as a label in psqlgraph
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file
        :param str node_id: used for logging

        :returns: a list of edges
        """

        edges = {}
        if 'edges_by_property' not in params or not params.edges_by_property:
            return edges

        for edge_type, dst_params in params.edges_by_property.items():
            for dst_label, dst_kv in dst_params.items():
                dst_matches = {
                    key: self.xpath(
                        val, root, expected=False, text=True, single=True,
                        label='{}: {}'.format(node_type, node_id))
                    for key, val in dst_kv.items()}
                dsts = list(self.graph.node_lookup(
                    label=dst_label, property_matches=dst_matches).all())
                for dst in dsts:
                    edges[dst.node_id] = (dst.label, edge_type)
        return edges
