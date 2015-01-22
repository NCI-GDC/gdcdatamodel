import unittest
import os
import uuid
from psqlgraph import PsqlGraphDriver, PsqlEdge
from zug.datamodel.tcga_magetab_sync import TCGAMAGETABSyncer, get_submitter_id_and_rev
import pandas as pd


TEST_DIR = os.path.dirname(os.path.realpath(__file__))
FIXTURES_DIR = os.path.join(TEST_DIR, "magetab_fixtures")


class TestTCGAMAGETASync(unittest.TestCase):

    def setUp(self):
        self.driver = PsqlGraphDriver('localhost', 'test',
                                      'test', 'automated_test')

    def tearDown(self):
        with self.driver.engine.begin() as conn:
            conn.execute('delete from edges')
            conn.execute('delete from nodes')
            conn.execute('delete from voided_edges')
            conn.execute('delete from voided_nodes')
        self.driver.engine.dispose()

    def fake_archive_for(self, fixture):
        # TODO this is a total hack, come back and make it better at some point
        return {"archive_name": fixture + "fake_test_archive"}

    def create_aliquot(self, uuid, barcode):
        return self.driver.node_merge(
            node_id=uuid if uuid else str(uuid.uuid4()),
            label="aliquot",
            properties={
                "submitter_id": barcode,
                "source_center": "foo",
                "amount": 3.5,
                "concentration": 10.0
            }
        )

    def create_archive(self, archive):
        submitter_id, rev = get_submitter_id_and_rev(archive)
        return self.driver.node_merge(
            node_id=str(uuid.uuid4()),
            label="archive",
            properties={"submitter_id": submitter_id,
                        "revision": rev}
        )

    def create_file(self, file, archive=None):
        file = self.driver.node_merge(
            node_id=str(uuid.uuid4()),
            label="file",
            properties={
                "file_name": file,
                "md5sum": "bogus",
                "file_size": 0,
                "file_state": "live"
            },
            system_annotations={
                "source": "cghub" if not archive else "tcga_dcc"
            }
        )
        if archive:
            edge = PsqlEdge(
                label="member_of",
                src_id=file.node_id,
                dst_id=archive.node_id
            )
            self.driver.edge_insert(edge)

    def test_basic_magetab_sync(self):
        aliquot = self.create_aliquot("290f101e-ff47-4aeb-ad71-11cb6e6b9dde",
                                      "TCGA-OR-A5J5-01A-11R-A29W-13")
        archive = self.create_archive("bcgsc.ca_ACC.IlluminaHiSeq_miRNASeq.Level_3.1.1.0")
        self.create_file("TCGA-OR-A5J5-01A-11R-A29W-13_mirna.bam")
        self.create_file("TCGA-OR-A5J5-01A-11R-A29W-13.isoform.quantification.txt",
                         archive=archive)
        self.create_file("TCGA-OR-A5J5-01A-11R-A29W-13.mirna.quantification.txt",
                         archive=archive)
        syncer = TCGAMAGETABSyncer(self.fake_archive_for("basic.sdrf.txt"),
                                   pg_driver=self.driver, lazy=True)
        syncer.df = pd.read_table(os.path.join(FIXTURES_DIR, "basic.sdrf.txt"))
        syncer.sync()
        n_files = self.driver.node_lookup(label="file")\
                             .with_edge_to_node("data_from", aliquot).count()
        self.assertEqual(n_files, 3)
