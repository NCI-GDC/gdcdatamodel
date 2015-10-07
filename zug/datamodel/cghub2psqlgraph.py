import re
import json
import datetime
import psqlgraph
from psqlgraph import PolyNode, Node
from lxml import etree
from dateutil.parser import parse as date_parse
import calendar
from cdisutils.log import get_logger
from zug.datamodel import xml2psqlgraph, cghub_categorization_mapping

from gdcdatamodel.models import File, Center

deletion_states = ['suppressed', 'redacted']


def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return int(delta.total_seconds())


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def to_bool(val):
    possible_true_values = ['true', 'yes']
    possible_false_values = ['false', 'no']
    if val is None:
        return None
    if val.lower() in possible_true_values:
        return True
    elif val.lower() in possible_false_values:
        return False
    else:
        raise ValueError("Cannot convert {} to boolean".format(val))


TARGET_STUDY_RE = re.compile('(phs000218|phs0004\d\d)')
TCGA_STUDY_RE = re.compile('phs000178')


class cghub2psqlgraph(object):

    """
    """

    def __init__(self, xml_mapping, host, user,
                 password, database, node_validator=None,
                 edge_validator=None, ignore_missing_properties=True,
                 signpost=None):
        """

        """

        self.graph = []
        self.bam_index_regex = re.compile('(.*)\.bai')
        self.center_regex = re.compile('(.*-){6}(.*)')
        self.xml_mapping = json.loads(
            json.dumps(xml_mapping), object_hook=AttrDict)
        self.graph = psqlgraph.PsqlGraphDriver(
            host=host, user=user, password=password, database=database)
        if node_validator:
            self.graph.node_validator = node_validator
        if edge_validator:
            self.graph.edge_validator = edge_validator
        self.xml = xml2psqlgraph.xml2psqlgraph(
            xml_mapping, host, user, password, database,
            node_validator=node_validator,
            edge_validator=edge_validator,
            ignore_missing_properties=ignore_missing_properties)
        self.signpost = signpost  # should be a SignpostClient object
        self.reset()
        self.log = get_logger("cghub_file_sync")

    def rebase(self):
        """Similar to export in xml2psqlgraph, but re-writes changes onto the
        graph

        ..note: postcondition: node/edge state is cleared.

        """
        self.log.info('Handling {} nodes'.format(len(self.files_to_add)))
        with self.graph.session_scope():
            self.rebase_file_nodes()

        self.log.info('Handling {} edges'.format(
            len(self.edges) + len(self.related_to_edges)))
        with self.graph.session_scope():
            self.export_edges()
        self.reset()

    def reset(self):
        self.files_to_add = {}
        self.files_to_delete = []
        self.edges = {}
        self.related_to_edges = {}

    def get_file_by_key(self, file_key):
        analysis_id, file_name = file_key
        return self.graph.nodes(File).props({'file_name': file_name})\
                                     .sysan({'analysis_id': analysis_id})\
                                     .scalar()

    def merge_file_node(self, file_key, node):
        """either create or update file record

        1. does this file_key already exist
        2a. if it does, then update it
        2b. if it does not, then get a new id for it, and add it

        """

        analysis_id, file_name = file_key
        node.sysan['analysis_id'] = analysis_id
        existing = self.get_file_by_key(file_key)

        if existing is not None:
            self.log.debug('Merging existing node {}'.format(file_key))
            # save node_id for later edge creation
            node.node_id = existing.node_id
            # save file state
            node.state = existing.state
            existing.props.update(node.props)
            existing.sysan.update(node.sysan)
            existing.acl = node.acl
        else:
            self.log.debug('Adding new {}'.format(file_key))
            node.node_id = self.signpost.create().did
            try:
                self.graph.current_session().add(node)
            except:
                self.log.error(node)
                self.log.error(node.properties)
                raise

        # Add the correct src_id to this file's edges now that we know it
        for edge in self.edges.get(file_key, []):
            edge.src_id = node.node_id
        return node.node_id

    def get_source(self, acl):
        if all([TCGA_STUDY_RE.match(phsid) for phsid in acl]):
            return 'tcga_cghub'
        elif all([TARGET_STUDY_RE.match(phsid) for phsid in acl]):
            return 'target_cghub'
        else:
            raise RuntimeError('Cant handle ACL {}'.format(acl))

    def delete_later(self, node):
        self.log.info("Marking %s as to_delete in system annotations", node)
        self.graph.node_update(
            node,
            system_annotations={
                "to_delete": True
            }
        )

    def rebase_file_nodes(self):
        """update file records in graph

        1. for each valid file, merge it in to the graph
        2. for each invalid file, remove it from the graph

        """

        # Loop through files to add and merge them into the graph
        for file_key, node in self.files_to_add.iteritems():
            node.sysan['source'] = self.get_source(node.acl)
            node_id = self.merge_file_node(file_key, node)
            self.files_to_add[file_key].node_id = node_id

        # Loop through files to remove and delete them from the graph
        for file_key in self.files_to_delete:
            node = self.get_file_by_key(file_key)
            if node:
                self.log.debug('Redacting {}'.format(file_key))
                self.delete_later(node)
            else:
                self.log.debug('Redaction not necessary {}'.format(file_key))

    def export_edge(self, edge):
        """
        Does this edge already exist? If not, insert it, else update it

        """
        with self.graph.session_scope() as session:
            if not self.graph.nodes().ids(str(edge.dst_id)).scalar():
                self.log.warn('Missing {} destination {}'.format(
                    edge.label, edge.dst_id))
            else:
                session.merge(edge)

    def export_edges(self):
        """Adds related_to edges then all other edges to psqlgraph from
        self.edges

        """
        for src_key, dst_key in self.related_to_edges.iteritems():
            src_id = self.files_to_add[src_key].node_id
            src_label = self.files_to_add[src_key].label
            dst_id = self.files_to_add[dst_key].node_id
            assert dst_id and src_id
            self.save_edge(
                src_key, dst_id, 'file', 'related_to', src_id=src_id,
                src_label=src_label)
        for src_f_name, edges in self.edges.iteritems():
            map(self.export_edge, edges)
        self.edges = {}

    def initialize(self, data):
        """Takes an xml string and performs xpath query to get result roots

        """
        if not data:
            return None
        self.xml_root = etree.fromstring(str(data)).getroottree()
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


        Steps:
        1. get analysis_id and filename as unique id
        2. parse literal properties from xml
        3. parse datetime properties from xml
        4. insert constant properties
        5. get the acl for the node
        6. check if file is live
        7. if live
           a. cache for later insertion
           b. start edge parsing
              i.   check if file is *.bam.bai
              ii.  if *.bam.bai, cache related to edge
              iii. if not *.bam.bai
                  1. look up edges from xml
                  2. cache edge for later insertion
        8. if not live
           a. cache for later suppression

        ..note: This function doesn't actually insert it into the
        graph.  You must call export after parse().

        :param str data: xml string to convert and insert

        """

        with self.graph.session_scope():
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
        file_key = self.get_file_key(root, node_type, params)
        args = (root, node_type, params, file_key)
        props = self.xml.get_node_properties(*args)
        props.update(self.xml.get_node_datetime_properties(*args))
        props.update(self.xml.get_node_const_properties(*args))
        acl = self.xml.get_node_acl(root, node_type, params)
        if all([TARGET_STUDY_RE.match(phsid) for phsid in acl]):
            # add the top-level TARGET phsid
            acl.append("phs000218")

        # Save the node for deletion or insertion
        state = self.get_file_node_state(*args)
        if state in deletion_states:
            if file_key not in self.files_to_delete:
                self.files_to_delete.append(file_key)
        elif state == 'live':
            self.categorize_file(root, file_key)
            node = self.save_file_node(file_key, node_type, props, acl)
            self.copy_result_key_to_sysan(root, node, "disease_abbr")
            self.copy_result_key_to_sysan(root, node, "legacy_sample_id")
            self.copy_result_key_to_sysan(root, node, "state")
            self.copy_result_key_to_sysan(root, node, "center_name")
            self.add_datetime_system_annotations(root, node)
            self.add_edges(root, node_type, params, file_key)
        else:
            node = self.get_file_by_key(file_key)
            if node:
                self.log.warn("File {} is in {} state but was ".format(
                    node, state) + "already in the graph. DELETING!")
            if file_key not in self.files_to_delete:
                self.files_to_delete.append(file_key)

    def copy_result_key_to_sysan(self, root, file_node, xml_key):
        datum = self.xml.xpath(
            'ancestor::Result/{}'.format(xml_key),
            root=root,
            single=True
        )
        file_node.merge(system_annotations={"cghub_"+xml_key: datum})

    def add_datetime_system_annotations(self, root, file_node):
        for key in ["last_modified", "upload_date", "published_date"]:
            val_as_iso8601 = self.xml.xpath(
                'ancestor::Result/{}'.format(key),
                root=root,
                single=True
            )
            val_as_seconds_since_epoch = calendar.timegm(
                date_parse(val_as_iso8601).timetuple())
            file_node.merge(
                system_annotations={"cghub_"+key: val_as_seconds_since_epoch}
            )

    def categorize_by_switch(self, root, cases):
        for dst_name, case in cases.iteritems():
            if None not in [re.match(condition['regex'], self.xml.xpath(
                    condition['path'], root, single=True, label=dst_name))
                    for condition in case.values()]:
                return dst_name
        raise RuntimeError('Unable to find correct categorization')

    def categorize_file(self, root, file_key):
        file_name = self.xml.xpath('./filename', root, single=True)
        if file_name.endswith('.bai') or file_name.endswith(".tar.bz2"):
            self.log.info("Not classifying %s", file_key)
            return

        self.save_center_edge(root, file_key)
        names = cghub_categorization_mapping['names']
        file_mapping = cghub_categorization_mapping['files']
        for dst_label, params in file_mapping.items():
            # Cases for type of parameter to get dst_name
            if 'const' in params:
                dst_name = params['const']
            elif 'path' in params:
                dst_name = self.xml.xpath(
                    params['path'], root, label=dst_label)[0]
            elif 'switch' in params:
                dst_name = self.categorize_by_switch(root, params['switch'])
            else:
                raise RuntimeError('File classification mapping is invalid')

            # Handle those without a destination name
            if not dst_name:
                self.log.warn('No desination from {} to {} found'.format(
                    file_name, dst_label))
                continue

            # Skip experimental strategies None and OTHER
            if dst_label == 'experimental_strategy' \
               and dst_name in [None, 'OTHER']:
                continue

            # Cache edge to categorization node
            normalized = names.get(dst_label, {}).get(str(dst_name), dst_name)
            dst = self.graph.nodes(Node.get_subclass(dst_label))\
                            .props(dict(name=normalized))\
                            .scalar()
            if not dst:
                self.log.warn('Missing dst {} name:{}, {}'.format(
                    dst_label, normalized, file_key))
            else:
                dst_id = dst.node_id
                edge_label = file_mapping[dst_label]['edge_label']
                self.save_edge(
                    file_key, dst_id, dst_label, edge_label, src_label='file')

    def save_center_edge(self, root, file_key):
        study = self.xml.xpath("ancestor::Result/study", root, single=True)
        # we need to branch on study because so far we've been relying
        # on parsing aliquot barcodes (legacy_sample_id) to tie TCGA
        # files to centers. this won't work for TARGET, so I'm simply
        # leaving the TCGA logic untouched in one half of this
        # conditional and implementing center_name based logic for
        # TARGET files in the other. Ideally we would do something
        # more unified but I'm doing this in the interest of not
        # breaking things
        if study == "phs000178":  # TCGA
            legacy_sample_id = self.xml.xpath('ancestor::Result/legacy_sample_id',
                                              root, single=True, nullable=True)
            if not legacy_sample_id:
                self.log.error('No legacy_sample_id for %s', file_key)
                return
            code = self.center_regex.match(legacy_sample_id)
            if not code:
                self.log.warn('Unable to parse center code from barcode: %s',
                              legacy_sample_id)
            else:
                center = self.graph.nodes(Center).props(code=code.group(2))\
                                                 .scalar()
                assert center, 'Missing center code:{}, {}'.format(
                    code.group(2), file_key)
                self.save_edge(
                    file_key, center.node_id, center.label, 'submitted_by',
                    src_label='file')
        else:  # TARGET
            center_name = self.xml.xpath("ancestor::Result/center_name",
                                         root, single=True, nullable=True)
            if not center_name:
                self.log.error('No center_name for %s', file_key)
                return
            if center_name == "CompleteGenomics":
                # note that this will stop working if we ever add more
                # CGI centers and this code will need to be updated to
                # disambiguate
                center = self.graph.nodes(Center).props(short_name="CGI").one()
            elif center_name == "BCCAGSC":
                # we have to specify code explicitly here because
                # there are two BCGSC CGCC centers, one of which (code
                # 31) is an accidentaly duplicate of the correct one
                # (code 13). we have to use the code here to make sure
                # we get the right one
                center = self.graph.nodes(Center).props(
                    code="13",
                ).one()
            elif center_name in ["BI", "BCM"]:
                center = self.graph.nodes(Center).props(
                    short_name=center_name,
                    center_type="CGCC",
                ).one()
            else:
                self.log.warning("File %s has unknown center_name: %s",
                                 file_key, center_name)
                return
            self.save_edge(
                file_key, center.node_id, center.label, 'submitted_by',
                src_label='file'
            )

    def add_edges(self, root, node_type, params, file_key):
        """
        i.   check if file is *.bam.bai
        ii.  if *.bam.bai, cache related to edge
        iii. if not *.bam.bai
            1. look up edges from xml
            2. cache edge for later insertion

        """
        analysis_id, file_name = file_key
        if self.is_bam_index_file(file_name):
            bam_file_name = self.bam_index_regex.match(file_name).group(1)
            self.related_to_edges[
                (analysis_id, bam_file_name)] = (analysis_id, file_name)
        else:
            edges = self.xml.get_node_edges(root, node_type, params)
            for dst_id, edge in edges.iteritems():
                dst_label, edge_label = edge
                self.save_edge(
                    file_key, dst_id, dst_label, edge_label, src_label='file')

    def is_bam_index_file(self, file_name):
        return self.bam_index_regex.match(file_name)

    def save_file_node(self, file_key, label, properties, acl=[]):
        """Adds an node to self.nodes_to_add

        If the file_key exists in the map, then update the node.  If
        it doesn't exist in the map, create it.

        """
        if file_key in self.files_to_add:
            self.files_to_add[file_key].merge(properties=properties)
        else:
            self.files_to_add[file_key] = PolyNode(
                node_id=None, acl=acl, label=label, properties=properties)
        return self.files_to_add[file_key]

    def save_edge(self, file_key, dst_id, dst_label, edge_label, src_id=None,
                  properties={}, src_label=None):
        """Adds an edge to self.edges

        If the file_key exists in the map, then append the edge to
        the file_key's list.  If it doesn't exist in the map, create
        it with a singleton containing the edge

        """
        assert src_label
        Type = self.graph.get_edge_by_labels(src_label, edge_label, dst_label)
        edge = Type(src_id=src_id, dst_id=dst_id, label=edge_label,
                    properties=properties)
        if file_key in self.edges:
            self.edges[file_key].append(edge)
        else:
            self.edges[file_key] = [edge]

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

    def get_file_key(self, root, node_type, params):
        """lookup the id for the node

        :param root: the lxml root element to treat as a node
        :param str node_type:
            the node type to be used as a label in psqlgraph
        :param dict params:
            the parameters that govern xpath queries and translation
            from the translation yaml file

        """
        file_name = self.xml.xpath(
            params.file_name, root, single=True, label=node_type)
        analysis_id = self.xml.xpath(
            params.properties.submitter_id.path, root,
            single=True, label=node_type)
        return (analysis_id, file_name)
