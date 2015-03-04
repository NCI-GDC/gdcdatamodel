import unittest
import os
from psqlgraph import PsqlGraphDriver
from zug.datamodel.target.sample_matrices import TARGETSampleMatrixSyncer
from zug.datamodel.prelude import create_prelude_nodes


TEST_DIR = os.path.dirname(os.path.realpath(__file__))
FIXTURES_DIR = os.path.join(TEST_DIR, "fixtures", "sample_matrices")


class TestTARGETSampleMatrixSync(unittest.TestCase):

    def setUp(self):
        self.driver = PsqlGraphDriver('localhost', 'test', 'test', 'automated_test')
        create_prelude_nodes(self.driver)

    def syncer_for(self, project):
        return TARGETSampleMatrixSyncer(project, graph=self.driver, dcc_auth=None)

    def tearDown(self):
        with self.driver.engine.begin() as conn:
            conn.execute('delete from edges')
            conn.execute('delete from nodes')
            conn.execute('delete from voided_edges')
            conn.execute('delete from voided_nodes')
        self.driver.engine.dispose()

    def trace_participant(self, aliquot_id):
        aliquot = self.driver.nodes().labels("aliquot")\
                                     .props({"submitter_id": aliquot_id}).one()
        sample = self.driver.nodes().labels("sample")\
                                    .with_edge_from_node("derived_from", aliquot).one()
        return self.driver.nodes().labels("participant")\
                                  .with_edge_from_node("derived_from", sample).one()

    def test_sync(self):
        syncer = self.syncer_for("AML")
        syncer.version = 1
        data = open(os.path.join(FIXTURES_DIR, "TARGET_AML_SampleMatrix_19910121.xlsx")).read()
        df = syncer.load_sample_matrix(data)
        mapping = syncer.compute_mapping_from_df(df)
        with self.driver.session_scope():
            syncer.put_mapping_in_pg(mapping)
        with self.driver.session_scope():
            self.trace_participant("TARGET-20-PABHET-03A-02R")
            self.trace_participant("TARGET-20-PABGKN-09A-01R")
        syncer.version = 2
        data = open(os.path.join(FIXTURES_DIR, "TARGET_AML_SampleMatrix_19910123.xlsx")).read()
        df = syncer.load_sample_matrix(data)
        mapping = syncer.compute_mapping_from_df(df)
        with self.driver.session_scope():
            syncer.put_mapping_in_pg(mapping)
            syncer.remove_old_versions()
        with self.driver.session_scope():
            self.assertEqual(self.driver.nodes().labels("aliquot").props({"submitter_id": "TARGET-20-PABHET-03A-02R"}).all(), [])
            self.trace_participant("TARGET-20-PABGKN-09A-01R")
            self.trace_participant("TARGET-20-PABHKY-03A-02R")
