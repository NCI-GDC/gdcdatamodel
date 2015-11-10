import json
import datetime
import psqlgraph
import pprint
from uuid import uuid5, UUID
from sqlalchemy.exc import IntegrityError
from psqlgraph import PolyNode as PsqlNode
from lxml import etree
from cdisutils.log import get_logger

log = get_logger(__name__)


possible_true_values = [
    'true',
    'yes',
]

possible_false_values = [
    'false',
    'no',
]

deletion_states = [
    'suppressed',
    'redacted',
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
    if val is None:
        return None
    if val.lower() in possible_true_values:
        return True
    elif val.lower() in possible_false_values:
        return False
    else:
        raise ValueError("Cannot convert {} to boolean".format(val))


class xml2psqlgraph(object):

    """
    """

    def __init__(self, xml_mapping, host, user,
                 password, database, node_validator=None,
                 edge_validator=None, ignore_missing_properties=True):
        """

        :param str translate_path:
            path to translation mapping yaml file

        """

        self.graph = []
        self.namespaces = None
        self.exported_nodes = 0
        self.export_count = 0
        self.ignore_missing_properties = ignore_missing_properties
        if xml_mapping:
            self.xml_mapping = json.loads(json.dumps(xml_mapping),
                                          object_hook=AttrDict)
        self.graph = psqlgraph.PsqlGraphDriver(
            host=host, user=user, password=password, database=database)
        self.nodes, self.edges = {}, {}

    def purge_old_nodes(self, group_id, version):
        with self.graph.session_scope() as s:
            group = self.graph.node_lookup(
                session=s, system_annotation_matches={'group_id': group_id})
            for node in group:
                if node.system_annotations['version'] < version:
                    log.warn('Found outdated node {}'.format(node))
                    self.graph.node_delete(node=node, session=s)

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
            log.error(result)
            raise Exception('{}: Expected 1 result for {}, found {}'.format(
                label, path, result))

        if text:
            result = [r.text for r in result]
            if not nullable and None in result:
                raise Exception('{}: Null result for {}'.format(label, result))

        if single:
            result = result[0]

        return result

    def export(self, silent=True, **kwargs):
        self.export_count += 1
        self.export_nodes(**kwargs)
        self.export_edges()
        if not silent:
            print 'Exports: {}. Nodes: {}. \r'.format(
                self.export_count, self.exported_nodes),

    def export_node(self, node, group_id=None, version=None,
                    system_annotations={}):

        if node.label == 'file':
            raise Exception(
                "Class xml2psqlgraph is not the right function "
                "to export files from. Try calling cghub2psqlgraph().")

        with self.graph.session_scope() as session:

            old_node = self.graph.node_lookup_one(node.node_id)

            if group_id and old_node and \
               old_node.system_annotations.get('group_id', None) != group_id:
                raise Exception(
                    'Group id {} does not match old {} for {}'.format(
                        group_id, old_node.system_annotations.get(
                            'group_id', None), node))

            if group_id is not None and version is not None:
                system_annotations.update(
                    {'group_id': group_id, 'version': version})

            node.system_annotations.update(system_annotations)
            session.merge(node)

    def export_nodes(self, **kwargs):
        for node_id, node in self.nodes.iteritems():
            try:
                self.export_node(node, **kwargs)
            except:
                log.error('Unable to add node {}'.format(node))
                pprint.pprint(node.properties)
                raise
            else:
                self.exported_nodes += 1
        self.nodes = {}

    def export_edges(self):
        with self.graph.session_scope() as session:
            for edge_id, e in self.edges.iteritems():
                try:
                    session.merge(e)
                except IntegrityError:
                    log.error('Unable to add edge {} from {} to {}'.format(
                        e.label, e.src, e.dst))
                    log.error(e.properties)
                    raise
        self.edges = {}

    def xml2psqlgraph(self, data):
        """Main function that takes xml string and converts it to a graph to
        insert into psqlgraph

        :param str data: xml string to convert and insert

        """

        if not data:
            return None
        self.xml_root = etree.fromstring(str(data)).getroottree()
        self.namespaces = self.xml_root.getroot().nsmap
        for node_type, param_list in self.xml_mapping.items():
            for params in param_list:
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
            # Get node and node properties
            node_id = self.get_node_id(root, node_type, params)
            args = (root, node_type, params, node_id)
            props = self.get_node_properties(*args)
            props.update(self.get_node_datetime_properties(*args))
            props.update(self.get_node_const_properties(*args))
            acl = self.get_node_acl(root, node_type, params)
            self.save_node(node_id, node_type, props, acl)

            # Get edges to and from this node
            edges = self.get_node_edges(root, node_type, params, node_id)
            for dst_id, edge in edges.iteritems():
                dst_label, edge_label = edge
                edge_props = {}
                edge_props.update(self.get_node_edge_properties(
                    root, edge_label, params, node_id))
                edge_props.update(self.get_node_edge_datetime_properties(
                    root, edge_label, params, node_id))
                self.save_edge(node_id, dst_id, dst_label, edge_label,
                               edge_props, src_label=node_type)

    def save_node(self, node_id, label, properties, acl=[]):
        """Adds a node to the graph

        """

        if label == 'file':
            raise Exception('xml2psqlgraph is not built to handle file nodes')

        if node_id in self.nodes:
            self.nodes[node_id].merge(properties=properties)
        else:
            self.nodes[node_id] = PsqlNode(
                node_id=node_id,
                acl=acl,
                label=label,
                properties=properties
            )

    def save_edge(self, src_id, dst_id, dst_label, edge_label,
                  properties={}, src_label=None):
        """Adds an edge to the graph

        """
        assert src_label
        edge_id = "{}:{}:{}".format(src_id, dst_id, edge_label)
        if edge_id in self.edges:
            self.edges[edge_id].merge(properties=properties)
        else:
            EdgeType = self.graph.get_edge_by_labels(
                src_label, edge_label, dst_label)
            self.edges[edge_id] = EdgeType(
                src_id=src_id,
                dst_id=dst_id,
                label=edge_label,
                properties=properties
            )

    def get_node_roots(self, node_type, params, root=None):
        """returns a list of xml node root elements for a given node_type

        :param str node_type:
            the node type to be used as a label in psqlgraph
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file

        """
        if not params.root:
            log.warn('No root xpath for {}'.format(node_type))
            return
        xml_nodes = self.xpath(
            params.root, root=root, expected=False,
            text=False, label='get_node_roots')
        return xml_nodes

    def get_node_acl(self, root, node_type, params):
        """lookup the id for the node

        :param root: the lxml root element to treat as a node
        :param str node_type:
            the node type to be used as a label in psqlgraph
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file

        """

        if 'acl' not in params or not params.acl:
            return []
        return self.xpath(params.acl, root, label=node_type)

    def get_node_id(self, root, node_type, params):
        """lookup the id for the node

        :param root: the lxml root element to treat as a node
        :param str node_type:
            the node type to be used as a label in psqlgraph
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file

        """

        assert not ('id' in params and 'generated_id' in params),\
            'Specification of an id xpath and parameters for generating an id'
        if 'id' in params:
            node_id = self.xpath(params.id, root, single=True, label=node_type)
        elif 'generated_id' in params:
            name = self.xpath(
                params.generated_id.name, root, single=True, label=node_type)
            node_id = str(uuid5(UUID(params.generated_id.namespace), name))
        else:
            raise LookupError('Unable to find id mapping in xml_mapping')
        return node_id.lower()

    def munge_property(self, prop, _type):
        types = {
            'int': int,
            'long': long,
            'float': float,
            'str': str,
            'str.lower': lambda x: str(x).lower(),
        }
        if _type == 'bool':
            prop = to_bool(prop)
        else:
            prop = types[_type](prop) if prop else prop
        return prop

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

        props = {}
        for prop, args in params.properties.items():
            if args is None:
                props[prop] = None
                continue
            path, _type = args['path'], args['type']
            if not path:
                continue
            result = self.xpath(
                path, root, single=True, text=True,
                expected=(not self.ignore_missing_properties),
                label='{}: {}'.format(node_type, node_id))
            props[prop] = self.munge_property(result, _type)
        return props

    def get_node_const_properties(self, root, node_type, params, node_id=''):
        """for each parameter in the setting file that is a constant value,
        add it to the properties dict

        :param root: the lxml root element to treat as a node
        :param str node_type:
            the node type to be used as a label in psqlgraph
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file
        :param str node_id: used for logging

        """

        if 'const_properties' not in params or not params.const_properties:
            return {}

        props = {}
        for prop, args in params.const_properties.items():
            props[prop] = self.munge_property(args['value'], args['type'])
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
                if not results:
                    log.warn('No {} edge for {} {} to {}'.format(
                        edge_type, node_type, node_id, dst_label))
                for result in results:
                    edges[result.lower()] = (dst_label, edge_type)
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
                with self.graph.session_scope() as session:
                    dsts = list(self.graph.node_lookup(
                        label=dst_label, property_matches=dst_matches,
                        session=session))
                    if not dsts:
                        log.warn('No {} edge for {} {} to {} with {}'.format(
                            edge_type, node_type, node_id,
                            dst_label, dst_matches))
                    session.expunge_all()
                for dst in dsts:
                    edges[dst.node_id] = (dst.label, edge_type)
        return edges

    def get_node_edge_properties(self, root, edge_type, params, node_id=''):
        if 'edge_properties' not in params or not params.edge_properties or \
           edge_type not in params.edge_properties:
            return {}

        props = {}
        for prop, args in params.edge_properties[edge_type].items():
            path, _type = args['path'], args['type']
            if not path:
                continue
            result = self.xpath(
                path, root, single=True, text=True,
                expected=(not self.ignore_missing_properties),
                label='{}: {}'.format(edge_type, node_id))
            props[prop] = self.munge_property(result, _type)
        return props

    def get_node_edge_datetime_properties(
            self, root, edge_type, params, node_id=''):

        if 'edge_datetime_properties' not in params \
           or not params.edge_datetime_properties \
           or edge_type not in params.edge_datetime_properties:
            return {}

        props = {}
        # Loop over all given datetime properties
        for name, timespans in params.edge_datetime_properties[edge_type]\
                                     .items():
            times = {'year': 0, 'month': 0, 'day': 0}

            # Parse the year, month, day
            for span in times:
                if span in timespans:
                    temp = self.xpath(
                        timespans[span], root, single=True, text=True,
                        expected=True,
                        label='{}: {}'.format(edge_type, node_id))
                    times[span] = 0 if temp is None else int(temp)

            if not times['year']:
                props[name] = 0
            else:
                props[name] = unix_time(datetime.datetime(
                    times['year'], times['month'], times['day']))
        return props
