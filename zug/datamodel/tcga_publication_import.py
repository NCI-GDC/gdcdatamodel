from zug.datamodel import PKG_DIR
import yaml
import os
from uuid import uuid5, UUID
from pandas import read_table
from cdisutils.log import get_logger
from psqlgraph import PsqlGraphDriver
from gdcdatamodel.models import Publication, File

PUBLICATION_NAMESPACE = UUID('b01299e1-4306-4fba-af07-2d6194320f10')


class TCGAPublicationImporter(object):
    '''Publication importer that create nodes for all
    publications and add publication - [refers_to] -> file edge
    to files specified in bamlist'''

    def __init__(self):
        self.logger = get_logger('tcga_publication_build')
        self.graph = PsqlGraphDriver(
            os.environ["ZUGS_PG_HOST"], os.environ["ZUGS_PG_USER"],
            os.environ["ZUGS_PG_PASS"], os.environ["ZUGS_PG_NAME"])

        self.publications = yaml.load(open(os.path.join(PKG_DIR,
                                      'publications.yml'), 'r'))
        self.bamlist = bamlist
        self.g = pg_driver
        self.logger = logger

    def run(self):
        with self.graph.session_scope() as session:
            self.create_publication_nodes(session)
            self.build_publication_edge(session)

    def create_publication_nodes(self, session):
        for disease, properties in self.publications.iteritems():
            node_id = str(uuid5(PUBLICATION_NAMESPACE, disease))
            pub_query = self.graph.nodes(Publication).ids(node_id)
            if pub_query.count() == 0:
                pub_node = Publication(node_id, properties=properties)
                pub_node = session.merge(pub_node)
            else:
                pub_node = pub_query.one()
            self.publications[disease]['pub_node'] = pub_node

    def get_missing_files(self):
        '''tool for collecting all missing files in bamlist'''

        print 'disease', 'filename', 'analysis_id'
        with self.graph.session_scope():
            for i in xrange(len(self.bamlist['disease'])):
                filename = self.bamlist['filename'][i]
                analysis_id = self.bamlist['cghub_uuid'][i]
                disease = self.bamlist['disease'][i]
                if analysis_id == analysis_id:
                    query = self.graph.nodes(File).sysan(
                        {'analysis_id': analysis_id})\
                        .props({'file_name': filename})
                else:
                    query = self.graph.nodes(File)\
                        .props({'file_name': filename})
                if query.count() == 0:
                    print bam['disease'], bam['filename'],\
                        bam.get('analysis_id', '')


    def build_publication_edge(self, session):
        for i in xrange(len(self.bamlist['disease'])):
            filename = self.bamlist['filename'][i]
            analysis_id = self.bamlist['cghub_uuid'][i]
            disease = self.bamlist['disease'][i]
            if analysis_id == analysis_id:

                query = self.graph.nodes(File).sysan(
                    {'analysis_id': analysis_id})\
                    .props({'file_name': filename})
            else:
                query = self.graph.nodes(File)\
                    .props({'file_name': filename})

            count = query.count()
            if count == 1:
                dst = query.one()
                src = self.publications[disease]['pub_node']
                if src not in dst.publications:
                    dst.publications.append(src)
                    session.merge(dst)
            else:
                self.logger.warn('filename {} has {} instance in graph'.
                                 format(bam['filename'], count))
