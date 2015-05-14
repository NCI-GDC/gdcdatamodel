from zug.datamodel import PKG_DIR
import yaml
import os
from uuid import uuid5, UUID
from psqlgraph import Edge
from pandas import read_table
from cdisutils.log import get_logger
from psqlgraph import PsqlGraphDriver
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from gdcdatamodel import node_avsc_object, edge_avsc_object


PUBLICATION_NAMESPACE = UUID('b01299e1-4306-4fba-af07-2d6194320f10')


class TCGAPublicationImporter(object):
    '''Publication importer that create nodes for all
    publications and add publication - [refers_to] -> file edge
    to files specified in bamlist'''

    def __init__(self):
        self.logger = get_logger('tcga_publication_build')
        self.graph = PsqlGraphDriver(
            os.environ["ZUGS_PG_HOST"], os.environ["ZUGS_PG_USER"],
            os.environ["ZUGS_PG_PASS"], os.environ["ZUGS_PG_NAME"],
            edge_validator=AvroEdgeValidator(edge_avsc_object),
            node_validator=AvroNodeValidator(node_avsc_object))

        self.publications = yaml.load(open(os.path.join(PKG_DIR,
                                      'publications.yml'), 'r'))
        self.bamlist = read_table(
            os.path.join(PKG_DIR, 'publication_bamlist.txt'))

    def run(self):
        with self.graph.session_scope():
            self.create_publication_nodes()
            self.build_publication_edge()

    def create_publication_nodes(self):
        for disease, properties in self.publications.iteritems():
            node_id = str(uuid5(PUBLICATION_NAMESPACE, disease))
            if self.graph.nodes().ids(node_id).count() == 0:
                self.graph.node_merge(
                    node_id=node_id,
                    properties=properties, label='publication')
            self.publications[disease]['node_id'] = node_id

    def get_missing_files(self):
        '''tool for collecting all missing files in bamlist'''

        print 'disease', 'filename', 'analysis_id'
        with self.graph.session_scope():
            for i in xrange(len(self.bamlist['disease'])):
                filename = self.bamlist['filename'][i]
                analysis_id = self.bamlist['cghub_uuid'][i]
                disease = self.bamlist['disease'][i]
                if analysis_id == analysis_id:
                    query = self.graph.nodes().sysan(
                        {'analysis_id': analysis_id})\
                        .props({'file_name': filename})
                else:
                    query = self.graph.nodes()\
                        .props({'file_name': filename})
                if query.count() == 0:
                    print disease, filename, analysis_id

    def build_publication_edge(self):
        for i in xrange(len(self.bamlist['disease'])):
            filename = self.bamlist['filename'][i]
            analysis_id = self.bamlist['cghub_uuid'][i]
            disease = self.bamlist['disease'][i]
            if analysis_id == analysis_id:

                query = self.graph.nodes().sysan(
                    {'analysis_id': analysis_id})\
                    .props({'file_name': filename})
            else:
                query = self.graph.nodes()\
                    .props({'file_name': filename})

            count = query.count()
            if count == 1:
                src_id = self.publications[disease]['node_id']
                dst_id = query.first().node_id
                if not self.graph.edge_lookup_one(src_id=src_id,
                                                  dst_id=dst_id,
                                                  label='refers_to'):
                    self.graph.edge_insert(Edge(
                        src_id=src_id,
                        dst_id=dst_id,
                        label='refers_to'))
            else:
                self.logger.warn('filename {} has {} instance in graph'.
                                 format(filename, count))
