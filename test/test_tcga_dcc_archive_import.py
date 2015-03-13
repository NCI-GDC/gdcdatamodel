from base import ZugsTestBase

from zug.datamodel.tcga_dcc_sync import TCGADCCArchiveSyncer
from zug.datamodel.latest_urls import LatestURLParser


class TCGADCCArchiveSyncTest(ZugsTestBase):

    def setUp(self):
        super(TCGADCCArchiveSyncTest, self).setUp()
        self.parser = LatestURLParser()
        self.storage_client.create_container("tcga_dcc_public")
        self.storage_client.create_container("tcga_dcc_protected")

    def syncer_for(self, archive, **kwargs):
        return TCGADCCArchiveSyncer(
            archive,
            signpost=self.signpost_client,
            pg_driver=self.graph,
            scratch_dir=self.scratch_dir,
            storage_client=self.storage_client,
            **kwargs
        )

    def test_syncing_an_archive_with_distinct_center(self):
        archive = self.parser.parse_archive(
            "hms.harvard.edu_BLCA.IlluminaHiSeq_DNASeqC.Level_3.1.3.0",
            "12/16/2013",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/blca/cgcc/hms.harvard.edu/illuminahiseq_dnaseqc/cna/hms.harvard.edu_BLCA.IlluminaHiSeq_DNASeqC.Level_3.1.3.0.tar.gz"
        )
        syncer = self.syncer_for(archive)
        syncer.sync()
       # make sure archive gets tied to project
        with self.graph.session_scope():
            # make sure the files get ties to center

            file = self.graph.node_lookup(
                label="file",
                property_matches={"file_name":"TCGA-BT-A0S7-01A-11D-A10R-02_AC1927ACXX---TCGA-BT-A0S7-10A-01D-A10R-02_AC1927ACXX---Segment.tsv"}
                ).one()
            center = self.graph.node_lookup(label="center").with_edge_from_node("submitted_by",file).one()
            self.assertEqual(center['code'],'02')
            # make sure file and archive are in storage
        self.storage_client.get_object("tcga_dcc_public", "/".join([archive["archive_name"], file["file_name"]]))
        self.storage_client.get_object("tcga_dcc_public", "/".join(["archives", archive["archive_name"]]))

    def test_syncing_an_archive_works(self):
        archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0",
            "11/12/2014",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0.tar.gz"
        )
        syncer = self.syncer_for(archive)
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
        self.storage_client.get_object("tcga_dcc_public", "/".join([archive["archive_name"], file["file_name"]]))
        self.storage_client.get_object("tcga_dcc_public", "/".join(["archives", archive["archive_name"]]))

    def test_syncing_is_idempotent(self):
        archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0",
            "11/12/2014",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0.tar.gz"
        )
        syncer = self.syncer_for(archive)
        syncer.sync()
        # change some of the data to be bunk to verify that we fix it
        with self.graph.session_scope():
            archive_node = self.graph.node_lookup(label="archive").one()
            archive_node.acl = ["foobar"]
            file = self.graph.node_lookup(
                label="file",
                property_matches={"file_name": "mdanderson.org_PAAD.MDA_RPPA_Core.protein_expression.Level_3.1C42FC2D-73FD-4EB4-9D02-294C2DB75D50.txt"}
            ).one()
            file.acl = ["fizzbuzz"]
        with self.graph.session_scope():
            first_archive_id = self.graph.node_lookup(label="archive").one().node_id
        syncer = self.syncer_for(archive, force=True)  # you can't reuse a syncer
        syncer.sync()
        with self.graph.session_scope():
            self.assertEqual(self.graph.node_lookup(label="file").count(), 109)
            self.assertEqual(self.graph.node_lookup(label="archive").count(), 1)
            self.assertEqual(self.graph.node_lookup(label="archive").one().node_id, first_archive_id)
        with self.graph.session_scope():
            archive_node = self.graph.node_lookup(label="archive").one()
            self.assertEqual(archive_node.acl, ["open"])
            file = self.graph.node_lookup(
                label="file",
                property_matches={"file_name": "mdanderson.org_PAAD.MDA_RPPA_Core.protein_expression.Level_3.1C42FC2D-73FD-4EB4-9D02-294C2DB75D50.txt"}
            ).one()
            self.assertEqual(file.acl, ["open"])

    def test_syncing_works_without_downloading(self):
        archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0",
            "11/12/2014",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0.tar.gz"
        )
        syncer = self.syncer_for(archive, meta_only=True)
        syncer.sync()
        with self.graph.session_scope():
            self.assertEqual(self.graph.node_lookup(label="file").count(), 109)
            self.assertEqual(self.graph.node_lookup(label="archive").count(), 1)
            archive_node = self.graph.node_lookup(label="archive").one()
            file = self.graph.node_lookup(label="file",
                                              property_matches={"file_name": "mdanderson.org_PAAD.MDA_RPPA_Core.protein_expression.Level_3.1C42FC2D-73FD-4EB4-9D02-294C2DB75D50.txt"})\
                                 .first()
        assert file["file_size"] == 5393
        assert file["state"] == "submitted"  # since it wasn't uploaded to object store
        # make sure archive gets tied to project
        with self.graph.session_scope():
            self.graph.node_lookup(label="project", property_matches={"code": "PAAD"})\
                          .with_edge_from_node("member_of", archive_node).one()
            # make sure the files get tied to classification stuff
            self.graph.node_lookup(label="data_subtype").with_edge_from_node("member_of", file).one()

    def test_replacing_old_archive_works(self):
        old_archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.1.0",
            "somedate",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.1.0.tar.gz")
        new_archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0",
            "11/12/2014",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0.tar.gz")

        syncer = self.syncer_for(old_archive)
        syncer.sync()
        syncer = self.syncer_for(new_archive)
        syncer.sync()
        with self.graph.session_scope():
            self.assertEqual(self.graph.node_lookup(label="file").count(), 109)
            self.assertEqual(self.graph.node_lookup(label="archive").count(), 1)
