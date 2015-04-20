import os
import random
from base import ZugsTestBase
from mock import patch
from httmock import urlmatch, HTTMock

from zug.datamodel.tcga_dcc_sync import TCGADCCArchiveSyncer
from zug.datamodel.latest_urls import LatestURLParser

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
FIXTURES_DIR = os.path.join(TEST_DIR, "fixtures", "dcc_archives")

ARCHIVES = {}

for archive in os.listdir(FIXTURES_DIR):
    ARCHIVES[archive] = open(os.path.join(FIXTURES_DIR, archive)).read()


@urlmatch(netloc=r'https://tcga-data.nci.nih.gov/.*')
def dcc_archives_fixture(url, request):
    archive = url.split("/")[-1]
    return {"content": ARCHIVES[archive],
            "status_code": 200}


class TCGADCCArchiveSyncTest(ZugsTestBase):

    def setUp(self):
        super(TCGADCCArchiveSyncTest, self).setUp()
        self.parser = LatestURLParser()
        self.storage_client.create_container("test_tcga_dcc_public")
        self.storage_client.create_container("test_tcga_dcc_protected")
        os.environ["PG_HOST"] = "localhost"
        os.environ["PG_USER"] = "test"
        os.environ["PG_PASS"] = "test"
        os.environ["PG_NAME"] = "automated_test"
        os.environ["SIGNPOST_URL"] = self.signpost_url
        os.environ["SCRATCH_DIR"] = self.scratch_dir
        os.environ["TCGA_PROTECTED_BUCKET"] = "test_tcga_dcc_protected"
        os.environ["TCGA_PUBLIC_BUCKET"] = "test_tcga_dcc_public"
        os.environ["DCC_USER"] = ""
        os.environ["DCC_PASS"] = ""

    def get_syncer(self):
        return TCGADCCArchiveSyncer(
            s3=self.storage_client,
            consul_prefix=str(random.randint(1,1000)) + "_test_tcgadccsync",
        )

    def test_syncing_an_archive_with_distinct_center(self):
        archive = self.parser.parse_archive(
            "hms.harvard.edu_BLCA.IlluminaHiSeq_DNASeqC.Level_3.1.3.0",
            "12/16/2013",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/blca/cgcc/hms.harvard.edu/illuminahiseq_dnaseqc/cna/hms.harvard.edu_BLCA.IlluminaHiSeq_DNASeqC.Level_3.1.3.0.tar.gz"
        )
        with patch("zug.datamodel.tcga_dcc_sync.LatestURLParser", lambda: [archive]), HTTMock(dcc_archives_fixture):
            syncer = self.get_syncer()
            syncer.sync()
        with self.graph.session_scope():
            file = self.graph.node_lookup(
                label="file",
                property_matches={"file_name":"TCGA-BT-A0S7-01A-11D-A10R-02_AC1927ACXX---TCGA-BT-A0S7-10A-01D-A10R-02_AC1927ACXX---Segment.tsv"}
                ).one()
            center = self.graph.node_lookup(label="center").with_edge_from_node("submitted_by",file).one()
            self.assertEqual(center['code'],'02')
            # make sure file and archive are in storage
        self.storage_client.get_object("test_tcga_dcc_public", "/".join([archive["archive_name"], file["file_name"]]))
        self.storage_client.get_object("test_tcga_dcc_public", "/".join(["archives", archive["archive_name"]]))

    def test_basic_sync(self):
        archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0",
            "11/12/2014",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0.tar.gz"
        )
        with patch("zug.datamodel.tcga_dcc_sync.LatestURLParser", lambda: [archive]), HTTMock(dcc_archives_fixture):
            syncer = self.get_syncer()
            syncer.sync()
        with self.graph.session_scope():
            self.assertEqual(self.graph.node_lookup(label="file").count(), 109)
            self.assertEqual(self.graph.node_lookup(label="archive").count(), 1)
            archive_node = self.graph.node_lookup(label="archive").one()
            file = self.graph.node_lookup(
                label="file",
                property_matches={"file_name": "mdanderson.org_PAAD.MDA_RPPA_Core.protein_expression.Level_3.1C42FC2D-73FD-4EB4-9D02-294C2DB75D50.txt"}
            ).one()
        assert file["file_size"] == 5393
        assert file["state"] == "live"
        # make sure archive gets tied to project
        with self.graph.session_scope():
            self.graph.node_lookup(label="project", property_matches={"code": "PAAD"})\
                      .with_edge_from_node("member_of", archive_node).one()
            # make sure the files get tied to classification stuff
            self.graph.node_lookup(label="data_subtype").with_edge_from_node("member_of", file).one()
            center = self.graph.node_lookup(label="center").with_edge_from_node("submitted_by",file).one()
            self.assertEqual(center['code'],'20')

        # make sure file and archive are in storage
        self.storage_client.get_object("test_tcga_dcc_public", "/".join([archive["archive_name"], file["file_name"]]))
        self.storage_client.get_object("test_tcga_dcc_public", "/".join(["archives", archive["archive_name"]]))

    def test_syncing_is_idempotent(self):
        archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0",
            "11/12/2014",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0.tar.gz"
        )
        with patch("zug.datamodel.tcga_dcc_sync.LatestURLParser", lambda: [archive]), HTTMock(dcc_archives_fixture):
            syncer = self.get_syncer()
            syncer.sync()
        # change some of the data to be bunk to verify that we fix it
        with self.graph.session_scope():
            archive_node = self.graph.node_lookup(label="archive").one()
            archive_node.acl = ["foobar"]
            self.graph.node_update(
                archive_node,
                system_annotations={"uploaded": False}
            )
            file = self.graph.node_lookup(
                label="file",
                property_matches={"file_name": "mdanderson.org_PAAD.MDA_RPPA_Core.protein_expression.Level_3.1C42FC2D-73FD-4EB4-9D02-294C2DB75D50.txt"}
            ).one()
            file.acl = ["fizzbuzz"]
        with self.graph.session_scope():
            first_archive_id = self.graph.node_lookup(label="archive").one().node_id
        with patch("zug.datamodel.tcga_dcc_sync.LatestURLParser", lambda: [archive]), HTTMock(dcc_archives_fixture):
            syncer = self.get_syncer()
            syncer.sync()
        with self.graph.session_scope():
            self.assertEqual(self.graph.node_lookup(label="file").not_sysan({"to_delete": True}).count(), 109)
            self.assertEqual(self.graph.node_lookup(label="archive").not_sysan({"to_delete": True}).count(), 1)
            self.assertEqual(self.graph.node_lookup(label="archive").one().node_id, first_archive_id)
        with self.graph.session_scope():
            archive_node = self.graph.node_lookup(label="archive").one()
            self.assertEqual(archive_node.acl, ["open"])
            file = self.graph.node_lookup(
                label="file",
                property_matches={"file_name": "mdanderson.org_PAAD.MDA_RPPA_Core.protein_expression.Level_3.1C42FC2D-73FD-4EB4-9D02-294C2DB75D50.txt"}
            ).one()
            self.assertEqual(file.acl, ["open"])

    def test_replacing_old_archive_works(self):
        old_archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.1.0",
            "somedate",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.1.0.tar.gz")
        new_archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0",
            "11/12/2014",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0.tar.gz")

        with patch("zug.datamodel.tcga_dcc_sync.LatestURLParser", lambda: [old_archive]), HTTMock(dcc_archives_fixture):
            syncer = self.get_syncer()
            syncer.sync()
        with patch("zug.datamodel.tcga_dcc_sync.LatestURLParser", lambda: [new_archive]), HTTMock(dcc_archives_fixture):
            syncer = self.get_syncer()
            syncer.sync()
        with self.graph.session_scope():
            self.assertEqual(self.graph.node_lookup(label="file").not_sysan({"to_delete": True}).count(), 109)
            self.assertEqual(self.graph.node_lookup(label="archive").not_sysan({"to_delete": True}).count(), 1)
