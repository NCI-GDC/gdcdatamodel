from base import ZugsTestBase
from zug.datamodel.tcga_publication_import import TCGAPublicationImporter
import uuid
import os
from gdcdatamodel.models import PublicationRefersToFile


class TestTCGAPublicationImport(ZugsTestBase):

    def setUp(self):
        super(TestTCGAPublicationImport, self).setUp()
        os.environ["ZUGS_PG_HOST"] = 'localhost'
        os.environ["ZUGS_PG_USER"] = 'test'
        os.environ["ZUGS_PG_PASS"] = 'test'
        os.environ["ZUGS_PG_NAME"] = 'automated_test'

    def create_file(self, file, sys_ann={}):
        file = self.graph.node_merge(
            node_id=str(uuid.uuid4()),
            label="file",
            properties={
                "file_name": file,
                "md5sum": "bogus",
                "file_size": long(0),
                "state": "live",
                'submitter_id': 'test',
                'state_comment': 'test'
            },
            system_annotations=sys_ann
        )
        return file

    def test_publication_import(self):
        importer = TCGAPublicationImporter([], self.driver, self.import_logger)
        importer.run()
        with self.graph.session_scope():
            pubs = importer.publications
            for key in pubs:
                del pubs[key]['node_id']
            self.graph.nodes().labels('publication').props(pubs['BLCA']).one()
            self.graph.nodes().labels('publication').props(pubs['BRCA']).one()
            self.graph.nodes().labels('publication').\
                props(pubs['COADREAD']).one()
            self.graph.nodes().labels('publication').props(pubs['HNSC']).one()
            self.graph.nodes().labels('publication').props(pubs['LUAD']).one()
            self.graph.nodes().labels('publication').props(pubs['OV']).one()
            self.graph.nodes().labels('publication').props(pubs['UCEC']).one()

    def test_publication_edge_build(self):
        file1 = self.create_file('test1', {'analysis_id': 'a'})
        file2 = self.create_file('test2', {'analysis_id': 'b'})
        file3 = self.create_file('test3', {'analysis_id': 'c'})
        bamlist = [{'filename': 'test1', 'analysis_id': 'a', 'disease': 'OV'},
                   {'filename': 'test2',
                    'analysis_id': 'b', 'disease': 'BLCA'},
                   {'filename': 'test3', 'disease': 'BLCA'}]
        importer = TCGAPublicationImporter(bamlist,
                                           self.driver, self.import_logger)
        importer.run()
        with self.graph.session_scope():
            self.graph.edges(PublicationRefersToFile).filter(
                PublicationRefersToFile.src_id ==
                importer.publications['OV']['node_id'])\
                .filter(
                PublicationRefersToFile.dst_id ==
                file1.node_id
            ).one()
            self.graph.edges(PublicationRefersToFile).filter(
                PublicationRefersToFile.src_id ==
                importer.publications['BLCA']['node_id'])\
                .filter(
                PublicationRefersToFile.dst_id == file2.node_id
            ).one()
            self.graph.edges(PublicationRefersToFile).filter(
                PublicationRefersToFile.src_id ==
                importer.publications['BLCA']['node_id'])\
                .filter(
                PublicationRefersToFile.dst_id == file3.node_id
            ).one()

    def test_importer_with_bam_not_in_graph(self):
        file1 = self.create_file('test1', {'analysis_id': 'a'})
        bamlist = [{'filename': 'test1', 'analysis_id': 'a', 'disease': 'OV'},
                   {'filename': 'test2',
                    'analysis_id': 'b', 'disease': 'BLCA'}]
        importer = TCGAPublicationImporter(bamlist,
                                           self.driver, self.import_logger)
        importer.run()
        with self.graph.session_scope():
            self.graph.edges(PublicationRefersToFile).filter(
                PublicationRefersToFile.src_id ==
                importer.publications['OV']['node_id'])\
                .filter(
                PublicationRefersToFile.dst_id == file1.node_id
            ).one()
            assert self.graph.edges().labels('refers_to').count() == 1

    def test_importer_with_duplicate_edge(self):
        file1 = self.create_file('test1', {'analysis_id': 'a'})
        bamlist = [{'filename': 'test1', 'analysis_id': 'a', 'disease': 'OV'},
                   {'filename': 'test1', 'analysis_id': 'a', 'disease': 'OV'}]
        importer = TCGAPublicationImporter(bamlist,
                                           self.driver, self.import_logger)
        importer.run()
        with self.graph.session_scope():
            self.graph.edges(PublicationRefersToFile).filter(
                PublicationRefersToFile.src_id ==
                importer.publications['OV']['node_id'])\
                .filter(
                PublicationRefersToFile.dst_id == file1.node_id
            ).one()
            assert self.graph.edges().labels('refers_to').count() == 1
