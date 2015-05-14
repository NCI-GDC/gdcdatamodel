from cdisutils.log import get_logger
from gdcdatamodel.models import File
import os


PROPS = [
    "_aliquot_uuid",
    "_aliquot_barcode",
    "_tumor_aliquot_uuid",
    "_tumor_aliquot_barcode",
    "_control_aliquot_uuid",
    "_control_aliquot_barcode",
    "_slide_uuid",
    "_slide_barcode",
    "_participant_uuid",
    "_participant_barcode",
    "_participant_barcode",
]


def sync(graph):
    """
    Build edges based on filename in the given db
    """
    # TODO maybe optimize this so it doesn't pull files that already
    # have edges from magetabs
    logger = get_logger("tcga_filename_metadata_sync")
    first = PROPS[0]
    rest = PROPS[1:]
    with graph.session_scope():
        q = graph.nodes(File).not_sysan({"to_delete": True}).has_sysan(first)
        for key in rest:
            q = q.union(graph.nodes(File).not_sysan({"to_delete": True}).has_sysan(key))
        logger.info("Loading files to process")
        files = q.all()
        logger.info("About to tie to biospecemin from filename for %s files", len(files))
        for file in files:
            syncer = TCGAFilenameMetadataSyncer(file, graph)
            syncer.build()


class TCGAFilenameMetadataSyncer(object):

    def __init__(self, file_node, graph):
        '''
        DCC to biospecimen edge builder class  which tie DCC file node to
        biospecimen nodes from file node's system annotation, if the file is
        not tied to any biospecimen from magetab parsing.

        '''
        self.file_node = file_node
        self.graph = graph
        self.log = get_logger('tcga_filename_metadata_syncer_'
                              + str(os.getpid()) + '_' +
                              self.file_node.node_id)

    def build(self):
        '''build edges between the given file node and biospecimen nodes'''
        with self.graph.session_scope():
            self.log.info("Building edges")
            self.tie_file_from_classification(self.file_node)

    def tie_file_from_classification(self, file_node):
        attrs = file_node.system_annotations
        self.log.debug(attrs)
        for edge in file_node.edges_out:
            if edge.label == 'data_from'\
               and edge.system_annotations['source'] == 'tcga_magetab':
                self.log.info("File already has edge from magetab: %s, bailing", edge)
                return
        nodes = []
        # find aliquot or slide node that should be tied to this file
        for possible_attr in ['_aliquot', '_tumor_aliquot', '_control_aliquot',
                              '_slide']:
            node = None
            if possible_attr + '_uuid' in attrs:
                node = self.graph.nodes().ids(
                    [attrs[possible_attr + '_uuid']]).scalar()
            elif possible_attr + '_barcode' in attrs:
                node = self.graph.nodes().props(
                    {'submitter_id': attrs[possible_attr + '_barcode']}).scalar()
            if node:
                self.log.info("found %s to tie to %s", node, self.file_node)
                nodes.append(node)

        # if no biospecimen is tied to this file, find participant that should
        # be tied to this file
        if not nodes:
            node = None
            if '_participant_uuid' in attrs:
                node = self.graph.nodes().ids(
                    [attrs['_participant_uuid']]).scalar()
            elif '_participant_barcode' in attrs:
                node = self.graph.nodes().props(
                    {'submitter_id': attrs['_participant_barcode']}).scalar()
            if node:
                self.log.info("found %s to tie to %s", node, self.file_node)
                nodes.append(node)

        for node in nodes:
            edge_to_biospecimen = self.graph.get_PsqlEdge(
                label='data_from',
                src_id=file_node.node_id,
                dst_id=node.node_id,
                src_label='file',
                dst_label=node.label,
            )
            edge_to_biospecimen.system_annotations['source'] = 'filename'
            self.log.info("inserting edge %s", edge_to_biospecimen)
            self.graph.edge_insert(edge_to_biospecimen)
