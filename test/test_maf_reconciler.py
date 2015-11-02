import os
from base import ZugTestBase, FakeS3Mixin, SignpostMixin, PreludeMixin, TEST_DIR
from mock import patch

from cdisutils.net import BotoManager
from boto.s3.connection import OrdinaryCallingFormat

from zug.datamodel.maf_reconciler import MAFReconciler

from gdcdatamodel.models import File, Aliquot, Analyte, Center, DataFormat


FIXTURES_DIR = os.path.join(TEST_DIR, "fixtures", "mafs")


class MAFReconcilerTest(SignpostMixin, FakeS3Mixin, PreludeMixin, ZugTestBase):

    def setUp(self):
        super(MAFReconcilerTest, self).setUp()
        self.setup_fake_s3("test")
        self.fake_s3.start()
        self.boto_manager = BotoManager({
            "s3.amazonaws.com": {
                "calling_format": OrdinaryCallingFormat()
            }
        })
        self.boto_manager
        self.fake_s3.stop()

    def make_reconciler(self):
        return MAFReconciler(
            graph=self.graph,
            signpost=self.signpost_client,
            s3=self.boto_manager,
        )

    def make_fake_maf(self, name, fixture):
        file_doc = self.signpost_client.create()
        file = self.get_fuzzed_node(
            File,
            node_id=file_doc.did,
            file_name=name,
            state="live"
        )
        file.sysan["source"] = "tcga_dcc"
        with self.graph.session_scope():
            maf_format = self.graph.nodes(DataFormat)\
                                   .props(name="MAF")\
                                   .one()
            file.data_formats = [maf_format]
        self.fake_s3.start()
        bucket = self.boto_manager["s3.amazonaws.com"].get_bucket("test")
        key = bucket.new_key(name)
        key.set_contents_from_string(
            open(os.path.join(FIXTURES_DIR, fixture)).read()
        )
        self.fake_s3.stop()
        file_doc.urls = ["s3://s3.amazonaws.com/test/{}".format(name)]
        file_doc.patch()
        return file

    def test_basic_reconcile(self):
        tumor_aliquot = self.get_fuzzed_node(
            Aliquot,
            node_id="29247427-bfe8-4b69-a31c-f913aed05832",
            submitter_id="tumor_barcode"
        )
        norm_aliquot = self.get_fuzzed_node(
            Aliquot,
            node_id="4b5944ef-144f-445c-919c-e831cd19ec4b",
            submitter_id="norm_barcode"
        )
        with self.graph.session_scope() as session:
            session.add(tumor_aliquot)
            session.add(norm_aliquot)
        file = self.make_fake_maf("test_maf.txt", "tcga_maf.txt")
        recon = self.make_reconciler()
        self.fake_s3.start()
        recon.reconcile(file)
        self.fake_s3.stop()
        with self.graph.session_scope() as session:
            session.add(file)
            norm_aliquot = session.merge(norm_aliquot)
            tumor_aliquot = session.merge(tumor_aliquot)
            self.assertIn(tumor_aliquot, file.aliquots)
            self.assertIn(norm_aliquot, file.aliquots)

    def test_reconcile_by_analyte_barcode(self):
        """Test that reconcling old MAFs with analyte barcodes using the
        method outlined by MAJ
        (https://jira.opensciencedatacloud.org/browse/GDC-635?focusedCommentId=15352&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-15352)
        works correctly.
        """
        aliquot = self.get_fuzzed_node(
            Aliquot,
            submitter_id="aliquot_barcode_not_used_in_reconciling"
        )
        analyte = self.get_fuzzed_node(
            Analyte,
            # the XXXXXX is to make it 20 chars since we use that as a
            # hint to find analytes
            submitter_id="tumor_barcode_XXXXXX",
            analyte_type="DNA",
            analyte_type_id="D",
        )
        with self.graph.session_scope() as session:
            center = self.graph.nodes(Center).props(short_name="WUSM").first()
            session.add(aliquot)
            session.add(analyte)
            analyte.aliquots = [aliquot]
            file = self.make_fake_maf(
                "analyte_test_maf.txt",
                "tcga_maf_with_analyte.txt",
            )
            file.centers = [center]
            aliquot.centers = [center]
        recon = self.make_reconciler()
        self.fake_s3.start()
        recon.reconcile(file)
        self.fake_s3.stop()
        with self.graph.session_scope() as session:
            session.add(file)
            aliquot = session.merge(aliquot)
            self.assertIn(aliquot, file.aliquots)

    def test_reconcile_all(self):
        """
        Test that the query for finding all MAFs to reconcile works
        """
        file = self.make_fake_maf("test_maf.txt", "tcga_maf.txt")
        recon = self.make_reconciler()
        with patch.object(recon, "reconcile") as mock:
            recon.reconcile_all()
            self.assertEqual(mock.call_args[0][0].node_id, file.node_id)
