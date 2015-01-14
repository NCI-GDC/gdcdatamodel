import yaml
import json
import uuid
import datetime
import logging
import psqlgraph
import pprint
from psqlgraph.edge import PsqlEdge
from psqlgraph.node import PsqlNode
from lxml import etree
import xml2psqlgraph

logger = logging.getLogger(name="[{name}]".format(name=__name__))

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


class cghub2psqlgraph(object):

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
        self.exported_nodes = 0
        self.export_count = 0
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
        self.edges, self.files_to_add, self.files_to_delete = {}, {}, []
        self.xml = xml2psqlgraph.xml2psqlgraph(
            translate_path, host, user, password, database,
            node_validator=self.node_validator,
            edge_validator=self.edge_validator)

    def rebase(self, source):
        """Similar to export in xml2psqlgraph, but re-writes changes onto the
        graph

        :param src source:
            the file source to be put in system_annotations

        """

        self.export_count += 1
        self.rebase_file_nodes(source)
        self.export_edges()
        print 'Exports: {}. Nodes: {}. \r'.format(
            self.export_count, self.exported_nodes),

    def get_existing_files(self, source, session):
        """dumps a list of files from a source to memory

        :param src source:
            the file source to be put in system_annotations

        """
        sa_matches = {'source': source}
        return {f['file_name']: f for f in
                self.graph.node_lookup_by_matches(
                    system_annotation_matches=sa_matches, label='file',
                    session=session).yield_per(1000)}

    def merge_file_node(self, existing_files, file_name, node,
                        session, system_annotations):
        """either create or update file record

        1. does this file_name already exist
        2a. if it does, then update it
        2b. if it does not, then get a new id for it, and add it

        :param src source:
            the file source to be put in system_annotations

        """

        existing = existing_files.get(file_name, None)
        if existing is not None:
            print('Updating {}'.format(file_name))
            node_id = existing.node_id
            self.graph.node_update(
                node=existing,
                properties=node.properties,
                system_annotations=system_annotations,
                session=session)
        else:
            print('Adding {}'.format(file_name))
            node_id = str(uuid.uuid4())
            node.node_id = node_id
            node.system_annotations.update(system_annotations)
            self.graph.node_insert(node=node, session=session)

        # Add the correct src_id to this file's edges now that we know it
        for edge in self.edges[file_name]:
            edge.src_id = node_id

        self.exported_nodes += 1

    def rebase_file_nodes(self, source):
        """update file records in graph

        1. for each valid file, merge it in to the graph
        2. for each invalid file, remove it from the graph

        ..note: postcondition: self.edges is cleared.

        :param src source:
            the file source to be put in system_annotations

        """

        system_annotations = {'source': source}
        with self.graph.session_scope() as session:
            existing_files = self.get_existing_files(
                source, session)
            print('Found {} existing files'.format(len(existing_files)))
            for file_name, node in self.files_to_add.iteritems():
                self.merge_file_node(existing_files, file_name, node,
                                     session, system_annotations)
            for file_name in self.files_to_delete:
                node = existing_files.get(file_name, None)
                if node:
                    print('Redacting {}'.format(file_name))
                    self.graph.node_delete(node=node, session=session)
                else:
                    print('Redaction not necessary {}'.format(file_name))
        self.files_to_add, self.files_to_delete = {}, []

    def export_edges(self):
        """Adds edges to psqlgraph from self.edges

        ..note: postcondition: self.edges is cleared.

        """

        with self.graph.session_scope() as session:
            for src_f_name, edges in self.edges.iteritems():
                for e in edges:
                    existing = self.graph.edge_lookup(
                        src_id=e.src_id, dst_id=e.dst_id, label=e.label,
                        session=session).count()
                    if not existing:
                        self.graph.edge_insert(e, session=session)
        self.edges = {}

    def parse(self, data):
        """Main function that takes xml string and converts it to a graph to
        insert into psqlgraph.

        ..note: This function doesn't actually insert it into the
        graph.  You must call export after parse().

        :param str data: xml string to convert and insert

        """

        if not data:
            return None
        self.xml_root = etree.fromstring(str(data)).getroottree()
        self.namespaces = self.xml_root.getroot().nsmap
        for node_type, params in self.translate.items():
            self.parse_file_node(node_type, params)
        print('Files to insert: {}. Files to redact {}'.format(
            len(self.files_to_add), len(self.files_to_delete)))

    def parse_file_node(self, node_type, params):
        """Convert a subsection of the xml that will be treated as a node

        :param str node_type: the type of node to be used as a label
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file

        """
        roots = self.get_node_roots(node_type, params)
        for root in roots:
            # Get node and node properties
            file_name = self.xml.get_file_name(root, node_type, params)
            args = (root, node_type, params, file_name)
            props = self.xml.get_node_properties(*args)
            props.update(self.xml.get_node_datetime_properties(*args))
            props.update(self.xml.get_node_const_properties(*args))
            acl = self.xml.get_node_acl(root, node_type, params)

            # Save the node for deletion or insertion
            state = self.get_file_node_state(*args)

            if state in deletion_states:
                self.files_to_delete.append(file_name)
            else:
                self.save_file_node(file_name, node_type, props, acl)

                # Get edges to and from this node
                edges = self.xml.get_node_edges(
                    root, node_type, params, file_name)
                for dst_id, edge in edges.iteritems():
                    dst_label, edge_label = edge
                    self.save_edge(file_name, dst_id, dst_label, edge_label)

    def save_file_node(self, file_name, label, properties, acl=[]):
        """Adds an node to self.nodes_to_add

        If the file_name exists in the map, then update the node.  If
        it doesn't exist in the map, create it.

        """

        properties.update({'file_name': file_name})
        if file_name in self.files_to_add:
            self.files_to_add[file_name].merge(properties=properties)
        else:
            self.files_to_add[file_name] = PsqlNode(
                node_id=None,
                acl=acl,
                label=label,
                properties=properties
            )

    def save_edge(self, file_name, dst_id, dst_label, edge_label,
                  properties={}):
        """Adds an edge to self.edges

        If the file_name exists in the map, then append the edge to
        the file_name's list.  If it doesn't exist in the map, create
        it with a singleton containing the edge

        """

        edge = PsqlEdge(
            src_id=None,
            dst_id=dst_id,
            label=edge_label,
            properties=properties
        )

        if file_name in self.edges:
            self.edges[file_name].append(edge)
        else:
            self.edges[file_name] = [edge]

    def get_file_node_state(self, root, node_type, params, node_id):
        """returns a filenode's state

        :param str node_type:
            the node type to be used as a label in psqlgraph
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file

        """
        if not params.state:
            raise Exception('No state xpath for {}'.format(node_type))
        return self.xml.xpath(params.state, root, single=True, label=node_type)

    def get_file_name(self, root, node_type, params):
        """lookup the id for the node

        :param root: the lxml root element to treat as a node
        :param str node_type:
            the node type to be used as a label in psqlgraph
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file

        """

        # File nodes
        file_name = self.xml.xpath(
            params.file_name, root, single=True, label=node_type)
        analysis_id = self.xml.xpath(
            params.analysis_id, root, single=True, label=node_type)
        return '{}/{}'.format(analysis_id, file_name)
