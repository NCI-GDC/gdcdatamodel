from base import ZugsTestBase
from zug.datamodel.tcga_publication_import import TCGAPublicationImporter
import uuid
import os
from gdcdatamodel.models import PublicationRefersToFile, Publication, File


class TestTCGAPublicationImport(ZugsTestBase):

    def setUp(self):
        super(TestTCGAPublicationImport, self).setUp()
        os.environ["ZUGS_PG_HOST"] = 'localhost'
        os.environ["ZUGS_PG_USER"] = 'test'
        os.environ["ZUGS_PG_PASS"] = 'test'
        os.environ["ZUGS_PG_NAME"] = 'automated_test'

    def create_file(self, session, filename, sys_ann={}):
        node = File(str(uuid.uuid4()),
            properties={
                "file_name": filename,
                "md5sum": "bogus",
                "file_size": long(0),
                "state": "live",
                'submitter_id': 'test',
                'state_comment': 'test'
            },
            system_annotations=sys_ann
        )
        session.merge(node)
        return node

    def test_publication_import(self):
        importer = TCGAPublicationImporter()
        importer.run()
        with self.graph.session_scope():
            pubs = importer.publications
            for key in pubs:
                del pubs[key]['pub_node']
            self.graph.nodes(Publication).props(pubs['BLCA']).one()
            self.graph.nodes(Publication).props(pubs['BRCA']).one()
            self.graph.nodes(Publication).\
                props(pubs['COADREAD']).one()
            self.graph.nodes(Publication).props(pubs['HNSC']).one()
            self.graph.nodes(Publication).props(pubs['LUAD']).one()
            self.graph.nodes(Publication).props(pubs['OV']).one()
            self.graph.nodes(Publication).props(pubs['UCEC']).one()

    def test_publication_edge_build(self):
        bamlist = {'disease': ['OV', 'BLCA', 'BLCA'],
                   'filename': ['test1', 'test2', 'test3'],
                   'cghub_uuid': ['a', 'b', 'c']}

        importer = TCGAPublicationImporter()
        importer.bamlist = bamlist
        with self.graph.session_scope() as session:
            file1 = self.create_file(session, 'test1', {'analysis_id': 'a'})
            file2 = self.create_file(session, 'test2', {'analysis_id': 'b'})
            file3 = self.create_file(session, 'test3', {'analysis_id': 'c'})
        importer.run()
        with self.graph.session_scope() as session:
            self.graph.edges(PublicationRefersToFile).filter(
                PublicationRefersToFile.src ==
                importer.publications['OV']['pub_node'])\
                .filter(
                PublicationRefersToFile.dst == file1
            ).one()
            self.graph.edges(PublicationRefersToFile).filter(
                PublicationRefersToFile.src ==
                importer.publications['BLCA']['pub_node'])\
                .filter(
                PublicationRefersToFile.dst == file2
            ).one()
            self.graph.edges(PublicationRefersToFile).filter(
                PublicationRefersToFile.src ==
                importer.publications['BLCA']['pub_node'])\
                .filter(
                PublicationRefersToFile.dst == file3
            ).one()

    def test_importer_with_bam_not_in_graph(self):
        bamlist = {'disease': ['OV', 'BLCA'],
                   'filename': ['test1', 'test2'],
                   'cghub_uuid': ['a', 'b']}

        importer = TCGAPublicationImporter()
        importer.bamlist = bamlist
        with self.graph.session_scope() as session:
            file1 = self.create_file(session, 'test1', {'analysis_id': 'a'})
        importer.run()
        with self.graph.session_scope() as session:
            self.graph.edges(PublicationRefersToFile).filter(
                PublicationRefersToFile.src ==
                importer.publications['OV']['pub_node'])\
                .filter(
                PublicationRefersToFile.dst == file1
            ).one()
            assert self.graph.edges(PublicationRefersToFile).count() == 1

    def test_importer_with_duplicate_edge(self):
        bamlist = {'disease': ['OV', 'OV'],
                   'filename': ['test1', 'test1'],
                   'cghub_uuid': ['a', 'a']}

        importer = TCGAPublicationImporter()
        importer.bamlist = bamlist
        with self.graph.session_scope() as session:
            file1 = self.create_file(session, 'test1', {'analysis_id': 'a'})
        importer.run()
        with self.graph.session_scope() as session:
            self.graph.edges(PublicationRefersToFile).filter(
                PublicationRefersToFile.src ==
                importer.publications['OV']['pub_node'])\
                .filter(
                PublicationRefersToFile.dst == file1
            ).one()
            assert self.graph.edges(PublicationRefersToFile).count() == 1
