import os
from gdcdatamodel.models import Participant, Aliquot
from zug.datamodel.target.sample_matrices import TARGETSampleMatrixSyncer
from base import ZugTestBase, TEST_DIR

FIXTURES_DIR = os.path.join(TEST_DIR, "fixtures", "sample_matrices")


class TestTARGETSampleMatrixSync(ZugTestBase):

    def syncer_for(self, project):
        return TARGETSampleMatrixSyncer(
            project, graph=self.graph, dcc_auth=None)

    def trace_participant(self, aliquot_id):
        return self.graph.nodes(Participant)\
                         .path('samples.aliquots')\
                         .props({"submitter_id": aliquot_id})\
                         .one()

    def test_sync(self):
        syncer = self.syncer_for("AML")
        syncer.version = 1
        data = open(os.path.join(
            FIXTURES_DIR, "TARGET_AML_SampleMatrix_19910121.xlsx")).read()
        df = syncer.load_sample_matrix(data)
        mapping = syncer.compute_mapping_from_df(df)
        with self.graph.session_scope():
            syncer.put_mapping_in_pg(mapping)
        with self.graph.session_scope():
            self.trace_participant("TARGET-20-PABHET-03A-02R")
            self.trace_participant("TARGET-20-PABGKN-09A-01R")
        syncer.version = 2
        data = open(os.path.join(
            FIXTURES_DIR, "TARGET_AML_SampleMatrix_19910123.xlsx")).read()
        df = syncer.load_sample_matrix(data)
        mapping = syncer.compute_mapping_from_df(df)
        with self.graph.session_scope():
            syncer.put_mapping_in_pg(mapping)
            syncer.remove_old_versions()
        with self.graph.session_scope():
            self.assertEqual(self.graph.nodes(Aliquot).props(
                {"submitter_id": "TARGET-20-PABHET-03A-02R"}).all(), [])
            self.trace_participant("TARGET-20-PABGKN-09A-01R")
            self.trace_participant("TARGET-20-PABHKY-03A-02R")
