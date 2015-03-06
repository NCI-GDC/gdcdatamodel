import unittest
import os
import uuid
from psqlgraph import PsqlGraphDriver, PsqlEdge, PsqlNode
from zug.datamodel.tcga_magetab_sync import TCGAMAGETABSyncer, get_submitter_id_and_rev
import pandas as pd


TEST_DIR = os.path.dirname(os.path.realpath(__file__))
FIXTURES_DIR = os.path.join(TEST_DIR, "fixtures", "magetabs")


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

    def fake_archive_for(self, fixture, rev=1):
        # TODO this is a total hack, come back and make it better at some point
        node = PsqlNode(
            node_id=str(uuid.uuid4()),
            label="archive",
            properties={
                "submitter_id": fixture + "fake_test_archive.1",
                "revision": rev
            }
        )
        self.driver.node_insert(node)
        return {
            "archive_name": fixture + "fake_test_archive.1." + str(rev) +".0",
            "disease_code": "FAKE",
            "batch": 1,
            "revision": rev
        }, node

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

    def create_portion(self, uuid, barcode):
        return self.driver.node_merge(
            node_id=uuid if uuid else str(uuid.uuid4()),
            label="portion",
            properties={
                "submitter_id": barcode,
                "portion_number": "30",
                "creation_datetime": 123456,
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
        fake_archive, fake_archive_node = self.fake_archive_for("basic.sdrf.txt")
        syncer = TCGAMAGETABSyncer(fake_archive, pg_driver=self.driver, lazy=True)
        syncer.df = pd.read_table(os.path.join(FIXTURES_DIR, "basic.sdrf.txt"))
        syncer.sync()
        with self.driver.session_scope():
            n_files = self.driver.node_lookup(label="file")\
                                 .with_edge_to_node("data_from", aliquot)\
                                 .with_edge_from_node("related_to", fake_archive_node).count()
        self.assertEqual(n_files, 2)

    def test_magetab_sync_deletes_old_edges(self):
        aliquot = self.create_aliquot("290f101e-ff47-4aeb-ad71-11cb6e6b9dde",
                                      "TCGA-OR-A5J5-01A-11R-A29W-13")
        archive = self.create_archive("bcgsc.ca_ACC.IlluminaHiSeq_miRNASeq.Level_3.1.1.0")
        self.create_file("TCGA-OR-A5J5-01A-11R-A29W-13_mirna.bam")
        self.create_file("TCGA-OR-A5J5-01A-11R-A29W-13.isoform.quantification.txt",
                         archive=archive)
        self.create_file("TCGA-OR-A5J5-01A-11R-A29W-13.mirna.quantification.txt",
                         archive=archive)
        fake_archive, fake_archive_node = self.fake_archive_for("basic.sdrf.txt", rev=1)
        syncer = TCGAMAGETABSyncer(fake_archive, pg_driver=self.driver, lazy=True)
        syncer.df = pd.read_table(os.path.join(FIXTURES_DIR, "basic.sdrf.txt"))
        syncer.sync()
        with self.driver.session_scope():
            self.driver.node_delete(node_id=fake_archive_node.node_id)
            old_edge_ids = set([edge.edge_id for edge in self.driver.edges().all()])

        fake_archive, _ = self.fake_archive_for("basic.sdrf.txt", rev=2)
        syncer = TCGAMAGETABSyncer(fake_archive, pg_driver=self.driver, lazy=True)
        syncer.df = pd.read_table(os.path.join(FIXTURES_DIR, "basic.sdrf.txt"))
        syncer.sync()
        with self.driver.session_scope():
            new_edge_ids = set([edge.edge_id for edge in self.driver.edges().all()])
        self.assertNotEqual(old_edge_ids, new_edge_ids)

    def test_magetab_sync_handles_missing_file_gracefully(self):
        # this is the same as the above test, but one of the files is missing
        aliquot = self.create_aliquot("290f101e-ff47-4aeb-ad71-11cb6e6b9dde",
                                      "TCGA-OR-A5J5-01A-11R-A29W-13")
        archive = self.create_archive("bcgsc.ca_ACC.IlluminaHiSeq_miRNASeq.Level_3.1.1.0")
        self.create_file("TCGA-OR-A5J5-01A-11R-A29W-13_mirna.bam")
        self.create_file("TCGA-OR-A5J5-01A-11R-A29W-13.isoform.quantification.txt",
                         archive=archive)
        fake_archive, _ = self.fake_archive_for("basic.sdrf.txt")
        syncer = TCGAMAGETABSyncer(fake_archive, pg_driver=self.driver, lazy=True)
        syncer.df = pd.read_table(os.path.join(FIXTURES_DIR, "basic.sdrf.txt"))
        syncer.sync()
        with self.driver.session_scope():
            n_files = self.driver.node_lookup(label="file")\
                                 .with_edge_to_node("data_from", aliquot).count()
        self.assertEqual(n_files, 1)

    def test_duplicate_barcode_magetab_sync(self):
        aliquot = self.create_aliquot(str(uuid.uuid4()), "TCGA-28-1751-01A-02R-0598-07")
        lvl1 = self.create_archive("unc.edu_GBM.AgilentG4502A_07_2.Level_1.1.6.0")
        lvl2 = self.create_archive("unc.edu_GBM.AgilentG4502A_07_2.Level_2.1.6.0")
        lvl3 = self.create_archive("unc.edu_GBM.AgilentG4502A_07_2.Level_3.1.6.0")
        self.create_file("US82800149_251780410508_S01_GE2_105_Dec08.txt",
                         archive=lvl1)
        self.create_file("US82800149_251780410508_S01_GE2_105_Dec08.txt_lmean.out.logratio.probe.tcga_level2.data.txt",
                         archive=lvl2)
        self.create_file("US82800149_251780410508_S01_GE2_105_Dec08.txt_lmean.out.logratio.gene.tcga_level3.data.txt",
                         archive=lvl3)
        fake_archive, _ = self.fake_archive_for("duplicate.sdrf.txt")
        syncer = TCGAMAGETABSyncer(fake_archive, pg_driver=self.driver, lazy=True)
        syncer.df = pd.read_table(os.path.join(FIXTURES_DIR, "duplicate.sdrf.txt"))
        syncer.sync()
        with self.driver.session_scope():
            n_files = self.driver.node_lookup(label="file")\
                                 .with_edge_to_node("data_from", aliquot).count()
        self.assertEqual(n_files, 3)

    def test_shipped_portion_magetab_sync(self):
        portion = self.create_portion("f9762bbb-bca0-4b54-a2c8-6f81a91de22f", "TCGA-OR-A5J2-01A-21-A39K-20")
        lvl1 = self.create_archive("mdanderson.org_ACC.MDA_RPPA_Core.Level_1.1.1.0")
        lvl2 = self.create_archive("mdanderson.org_ACC.MDA_RPPA_Core.Level_2.1.1.0")
        lvl3 = self.create_archive("mdanderson.org_ACC.MDA_RPPA_Core.Level_3.1.2.0")
        self.create_file("14-3-3_beta-R-V_GBL11066140.txt", archive=lvl1)
        self.create_file("14-3-3_beta-R-V_GBL11066140.tif", archive=lvl1)
        self.create_file("mdanderson.org_ACC.MDA_RPPA_Core.SuperCurve.Level_2.F9762BBB-BCA0-4B54-A2C8-6F81A91DE22F.txt",
                         archive=lvl2)
        self.create_file("mdanderson.org_ACC.MDA_RPPA_Core.protein_expression.Level_3.F9762BBB-BCA0-4B54-A2C8-6F81A91DE22F.txt",
                         archive=lvl3)
        fake_archive, _ = self.fake_archive_for("duplicate.sdrf.txt")
        syncer = TCGAMAGETABSyncer(fake_archive, pg_driver=self.driver, lazy=True)
        syncer.df = pd.read_table(os.path.join(FIXTURES_DIR, "protein_exp.sdrf.txt"))
        syncer.sync()
        with self.driver.session_scope():
            n_files = self.driver.node_lookup(label="file")\
                                 .with_edge_to_node("data_from", portion).count()
        self.assertEqual(n_files, 4)
