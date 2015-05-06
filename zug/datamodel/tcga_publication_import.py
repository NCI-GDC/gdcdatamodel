from zug.datamodel import PKG_DIR
import yaml
import os
from uuid import uuid5, UUID
from psqlgraph import Edge

PUBLICATION_NAMESPACE = UUID('b01299e1-4306-4fba-af07-2d6194320f10')


class TCGAPublicationImporter(object):
    '''Publication importer that create nodes for all
    publications and add publication - [refers_to] -> file edge
    to files specified in bamlist'''

    def __init__(self, bamlist, pg_driver, logger):
        self.publications = yaml.load(open(os.path.join(PKG_DIR,
                                      'publications.yml'), 'r'))
        self.bamlist = bamlist
        self.g = pg_driver
        self.logger = logger

    def run(self):
        with self.g.session_scope():
            self.create_publication_nodes()
            self.build_publication_edge()

    def create_publication_nodes(self):
        for disease, properties in self.publications.iteritems():
            node_id = str(uuid5(PUBLICATION_NAMESPACE, disease))
            if self.g.nodes().ids(node_id).count() == 0:
                self.g.node_merge(
                    node_id=node_id,
                    properties=properties, label='publication')
            self.publications[disease]['node_id'] = node_id

    def get_missing_files(self):
        '''tool for collecting all missing files in bamlist'''

        print 'disease', 'filename', 'analysis_id'
        with self.g.session_scope():
            for bam in self.bamlist:
                if 'analysis_id' in bam:
                    query = self.g.nodes().sysan(
                        {'analysis_id': bam['analysis_id']})\
                        .props({'file_name': bam['filename']})
                else:
                    query = self.g.nodes()\
                        .props({'file_name': bam['filename']})
                if query.count() == 0:
                    print bam['disease'], bam['filename'],\
                        bam.get('analysis_id', '')

    def build_publication_edge(self):
        for bam in self.bamlist:
            if 'analysis_id' in bam:
                query = self.g.nodes().sysan(
                    {'analysis_id': bam['analysis_id']})\
                    .props({'file_name': bam['filename']})
            else:
                query = self.g.nodes()\
                    .props({'file_name': bam['filename']})

            count = query.count()
            if count == 1:
                src_id = self.publications[bam['disease']]['node_id']
                dst_id = query.first().node_id
                if not self.g.edge_lookup_one(src_id=src_id,
                                              dst_id=dst_id,
                                              label='refers_to'):
                    self.g.edge_insert(Edge(
                        src_id=src_id,
                        dst_id=dst_id,
                        label='refers_to'))
            else:
                self.logger.warn('filename {} has {} instance in graph'.
                                 format(bam['filename'], count))
