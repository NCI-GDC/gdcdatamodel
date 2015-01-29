import re
import json
import uuid
import datetime
import logging
import psqlgraph
from psqlgraph.edge import PsqlEdge
from psqlgraph.node import PsqlNode
from lxml import etree
from cdisutils.log import get_logger
from zug.datamodel import xml2psqlgraph, cghub_categorization_mapping


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


class cghub2psqlgraph(object):

    """
    """

    def __init__(self, xml_mapping, host, user,
                 password, database, node_validator=None,
                 edge_validator=None, ignore_missing_properties=True):
        """

        """

        self.graph = []
        self.namespaces = None
        self.exported_nodes = 0
        self.export_count = 0
        self.bam_index_regex = re.compile('(.*)\.bai')
        self.ignore_missing_properties = ignore_missing_properties
        self.xml_mapping = json.loads(json.dumps(xml_mapping),
                                      object_hook=AttrDict)
        self.graph = psqlgraph.PsqlGraphDriver(
            host=host, user=user, password=password, database=database)
        if node_validator:
            self.graph.node_validator = node_validator
        if edge_validator:
            self.graph.edge_validator = edge_validator
        self.edges, self.files_to_add, self.files_to_delete = {}, {}, []
        self.related_to_edges = []
        self.xml = xml2psqlgraph.xml2psqlgraph(
            xml_mapping, host, user, password, database,
            node_validator=node_validator,
            edge_validator=edge_validator)

    def rebase(self, source):
        """Similar to export in xml2psqlgraph, but re-writes changes onto the
        graph

        :param src source:
            the file source to be put in system_annotations

        """

        self.export_count += 1
        self.rebase_file_nodes(source)
        self.export_edges()
        log.debug('Exports: {}. Nodes: {}.'.format(
            self.export_count, self.exported_nodes))
        self.reset()

    def reset(self):
        self.files_to_add, self.files_to_delete = {}, []

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
            log.debug('Merging {}'.format(file_name))
            node.node_id = existing.node_id
            self.graph.node_update(
                node=existing,
                properties=node.properties,
                system_annotations=system_annotations,
                session=session)
        else:
            log.debug('Adding {}'.format(file_name))
            node.node_id = str(uuid.uuid4())
            node.system_annotations.update(system_annotations)
            try:
                self.graph.node_insert(node=node, session=session)
            except:
                log.error(node)
                log.error(node.properties)
                raise

        # Add the correct src_id to this file's edges now that we know it
        for edge in self.edges.get(file_name, []):
            edge.src_id = node.node_id

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
            log.debug('Found {} existing files'.format(len(existing_files)))
            for file_name, node in self.files_to_add.iteritems():
                self.merge_file_node(existing_files, file_name, node,
                                     session, system_annotations)
            for file_name in self.files_to_delete:
                node = existing_files.get(file_name, None)
                if node:
                    log.debug('Redacting {}'.format(file_name))
                    self.graph.node_delete(node=node, session=session)
                else:
                    log.debug('Redaction not necessary {}'.format(
                        file_name))

    def export_edge(self, edge, session):
        existing = self.graph.edge_lookup(
            src_id=edge.src_id, dst_id=edge.dst_id, label=edge.label,
            session=session).count()
        if not existing:
            src = self.graph.node_lookup_one(
                node_id=edge.dst_id, session=session)
            if src:
                self.graph.edge_insert(edge, session=session)
            else:
                logging.warn('Missing destination {}'.format(edge.dst_id))
                src = self.graph.node_lookup_one(
                    node_id=edge.src_id, session=session)
                src.system_annotations.update(
                    {'missing_aliquot': edge.dst_id})

    def export_edges(self):
        """Adds related_to edges then all other edges to psqlgraph from
        self.edges

        ..note: postcondition: self.edges is cleared.

        """
        for src_name, dst_name in self.related_to_edges:
            self.save_edge(
                src_name, self.files_to_add[dst_name].node_id, 'file',
                'related_to', self.files_to_add[src_name].node_id)
        with self.graph.session_scope() as session:
            for src_f_name, edges in self.edges.iteritems():
                for edge in edges:
                    self.export_edge(edge, session)
        self.edges = {}

    def initialize(self, data):
        if not data:
            return None
        self.xml_root = etree.fromstring(str(data)).getroottree()
        self.namespaces = self.xml_root.getroot().nsmap
        self.node_roots = {}
        for node_type, param_list in self.xml_mapping.items():
            for params in param_list:
                self.node_roots[node_type] = self.xml.get_node_roots(
                    node_type, params, root=self.xml_root)

    def parse_all(self):
        for node_type, params in self.xml_mapping.items():
            for root in self.node_roots[node_type]:
                self.parse_file_node(node_type, root, params)

    def parse(self, node_type, root):
        """Main function that takes xml string and converts it to a graph to
        insert into psqlgraph.

        ..note: This function doesn't actually insert it into the
        graph.  You must call export after parse().

        :param str data: xml string to convert and insert

        """

        for params in self.xml_mapping[node_type]:
            files = self.xml.get_node_roots(node_type, params, root=root)
            for f in files:
                self.parse_file_node(f, node_type, params)

    def parse_file_node(self, root, node_type, params):
        """Convert a subsection of the xml that will be treated as a node

        :param str node_type: the type of node to be used as a label
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file

        """
        # Get node and node properties
        file_name = self.get_file_name(root, node_type, params)
        args = (root, node_type, params, file_name)
        props = self.xml.get_node_properties(*args)
        props.update(self.xml.get_node_datetime_properties(*args))
        props.update(self.xml.get_node_const_properties(*args))
        acl = self.xml.get_node_acl(root, node_type, params)
        print file_name
        self.categorize_file(root)

        # Save the node for deletion or insertion
        state = self.get_file_node_state(*args)
        if state in deletion_states:
            self.files_to_delete.append(file_name)
        else:
            node = self.save_file_node(file_name, node_type, props, acl)
            self.add_edges(root, node_type, params, file_name, node)

    def categorize_by_switch(self, root, cases):
        for dst_name, case in cases.iteritems():
            if None not in [
                re.match(
                    condition['regex'], self.xml.xpath(
                        condition['path'], root, single=True, label=dst_name))
                    for condition in case.values()]:
                return dst_name
        raise RuntimeError('Unable to find correct categorization')

    def categorize_file(self, root):
        if self.xml.xpath('./filename', root, single=True).endswith('.bai'):
            return
        mapping = cghub_categorization_mapping['mapping']
        parse = cghub_categorization_mapping['files']
        for dst_label, params in parse.iteritems():
            if 'const' in params:
                dst_name = params['const']
            elif 'path' in params:
                dst_name = self.xml.xpath(
                    params['path'], root, label=dst_label)[0]
            elif 'switch' in params:
                dst_name = self.categorize_by_switch(root, params['switch'])
                dst_name = None if '_IGNORED_' in dst_name else dst_name
            else:
                raise RuntimeError('File classification mapping is invalid')
            if not dst_name:
                continue
            if dst_label in mapping:
                dst_name = mapping[dst_label][dst_name]
            print dst_label, dst_name
            # dsts = list(self.graph.node_lookup(
            #     label=dst_label, property_matches={'name': dst_name}))
            # self.save_edge(file_name, dst_id, dst_label, edge_label)

    def add_edges(self, root, node_type, params, file_name, node):
        if self.is_bam_index_file(file_name):
            bam_file_name = self.bam_index_regex.match(file_name).group(1)
            self.related_to_edges.append((bam_file_name, file_name))
        else:
            # Get edges to and from this node
            edges = self.xml.get_node_edges(
                root, node_type, params, file_name)
            for dst_id, edge in edges.iteritems():
                dst_label, edge_label = edge
                self.save_edge(file_name, dst_id, dst_label, edge_label)

    def is_bam_index_file(self, file_name):
        return self.bam_index_regex.match(file_name)

    def save_file_node(self, file_name, label, properties, acl=[]):
        """Adds an node to self.nodes_to_add

        If the file_name exists in the map, then update the node.  If
        it doesn't exist in the map, create it.

        """

        properties.update({u'file_name': file_name})
        if file_name in self.files_to_add:
            node = self.files_to_add[file_name]
            node.merge(properties=properties)
        else:
            node = PsqlNode(
                node_id=None,
                acl=acl,
                label=label,
                properties=properties
            )
            self.files_to_add[file_name] = node

    def save_edge(self, file_name, dst_id, dst_label, edge_label, src_id=None,
                  properties={}):
        """Adds an edge to self.edges

        If the file_name exists in the map, then append the edge to
        the file_name's list.  If it doesn't exist in the map, create
        it with a singleton containing the edge

        """

        edge = PsqlEdge(
            src_id=src_id,
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
