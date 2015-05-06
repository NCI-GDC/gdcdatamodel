import unittest
from psqlgraph import PsqlGraphDriver
from zug.datamodel.tcga_publication_import import TCGAPublicationImporter
import uuid
from cdisutils.log import get_logger
from gdcdatamodel import node_avsc_object, edge_avsc_object
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator


class TestTCGAPublicationImport(unittest.TestCase):

    def setUp(self):
        self.driver = PsqlGraphDriver(
            'localhost', 'test', 'test', 'automated_test',
            edge_validator=AvroEdgeValidator(edge_avsc_object),
            node_validator=AvroNodeValidator(node_avsc_object))

        self.import_logger = get_logger('tcga_publication_import')

    def tearDown(self):
        with self.driver.engine.begin() as conn:
            conn.execute('delete from edges')
            conn.execute('delete from nodes')
            conn.execute('delete from voided_edges')
            conn.execute('delete from voided_nodes')
        self.driver.engine.dispose()

    def create_file(self, file, sys_ann={}):
        file = self.driver.node_merge(
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
        with self.driver.session_scope():
            pubs = importer.publications
            for key in pubs:
                del pubs[key]['node_id']
            self.driver.nodes().labels('publication').props(pubs['BLCA']).one()
            self.driver.nodes().labels('publication').props(pubs['BRCA']).one()
            self.driver.nodes().labels('publication').\
                props(pubs['COADREAD']).one()
            self.driver.nodes().labels('publication').props(pubs['HNSC']).one()
            self.driver.nodes().labels('publication').props(pubs['LUAD']).one()
            self.driver.nodes().labels('publication').props(pubs['OV']).one()
            self.driver.nodes().labels('publication').props(pubs['UCEC']).one()

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
        with self.driver.session_scope():
            self.driver.edge_lookup(
                label="refers_to",
                src_id=importer.publications['OV']['node_id'],
                dst_id=file1.node_id
            ).one()
            self.driver.edge_lookup(
                label="refers_to",
                src_id=importer.publications['BLCA']['node_id'],
                dst_id=file2.node_id
            ).one()
            self.driver.edge_lookup(
                label="refers_to",
                src_id=importer.publications['BLCA']['node_id'],
                dst_id=file3.node_id
            ).one()

    def test_importer_with_bam_not_in_graph(self):
        file1 = self.create_file('test1', {'analysis_id': 'a'})
        bamlist = [{'filename': 'test1', 'analysis_id': 'a', 'disease': 'OV'},
                   {'filename': 'test2',
                    'analysis_id': 'b', 'disease': 'BLCA'}]
        importer = TCGAPublicationImporter(bamlist,
                                           self.driver, self.import_logger)
        importer.run()
        with self.driver.session_scope():
            self.driver.edge_lookup(
                label="refers_to",
                src_id=importer.publications['OV']['node_id'],
                dst_id=file1.node_id
            ).one()
            assert self.driver.edges().count() == 1

    def test_importer_with_duplicate_edge(self):
        file1 = self.create_file('test1', {'analysis_id': 'a'})
        bamlist = [{'filename': 'test1', 'analysis_id': 'a', 'disease': 'OV'},
                   {'filename': 'test1', 'analysis_id': 'a', 'disease': 'OV'}]
        importer = TCGAPublicationImporter(bamlist,
                                           self.driver, self.import_logger)
        importer.run()
        with self.driver.session_scope():
            self.driver.edge_lookup(
                label="refers_to",
                src_id=importer.publications['OV']['node_id'],
                dst_id=file1.node_id
            ).one()
            assert self.driver.edges().count() == 1
