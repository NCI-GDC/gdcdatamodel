from unittest import TestCase
from mock import patch

import tempfile

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

from zug.datamodel.tcga_dcc_sync import TCGADCCArchiveSyncer
from zug.datamodel.latest_urls import LatestURLParser

from psqlgraph import PsqlGraphDriver


class TCGADCCArchiveSyncTest(TestCase):

    def setUp(self):
        self.pg_driver = PsqlGraphDriver('localhost', 'test',
                                         'test', 'automated_test')
        self.dcc_auth = None  # TODO test protected archives, passing pw in an env var
        self.scratch_dir = tempfile.mkdtemp()
        Local = get_driver(Provider.LOCAL)
        self.storage_client = Local(tempfile.mkdtemp())
        self.storage_client.create_container("tcga_dcc_public")
        self.storage_client.create_container("tcga_dcc_protected")
        self.signpost_url = "http://localhost:5000"
        self.syncer = TCGADCCArchiveSyncer(self.signpost_url, self.pg_driver,
                                           self.storage_client, self.dcc_auth,
                                           self.scratch_dir)
        self.parser = LatestURLParser()

    def tearDown(self):
        with self.pg_driver.engine.begin() as conn:
            conn.execute('delete from edges')
            conn.execute('delete from nodes')
            conn.execute('delete from voided_edges')
            conn.execute('delete from voided_nodes')
        self.pg_driver.engine.dispose()
        for container in self.storage_client.list_containers():
            for obj in container.list_objects():
                obj.delete()

    def test_syncing_an_archive_works(self):
        archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0",
            "11/12/2014",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0.tar.gz"
        )
        self.syncer.sync_archive(archive)
        self.assertEqual(self.pg_driver.node_lookup(label="file").count(), 109)
        self.assertEqual(self.pg_driver.node_lookup(label="archive").count(), 1)
        # make sure we uploaded the archive
        assert self.storage_client.get_object("tcga_dcc_public", "archives/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0")

    def test_syncing_is_idempotent(self):
        archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0",
            "11/12/2014",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0.tar.gz"
        )
        self.syncer.sync_archive(archive)
        self.syncer.sync_archive(archive)
        self.assertEqual(self.pg_driver.node_lookup(label="file").count(), 109)
        self.assertEqual(self.pg_driver.node_lookup(label="archive").count(), 1)

    @patch.object(TCGADCCArchiveSyncer, 'extract_manifest',
                  lambda _, __, ___: None)
    def test_syncing_handles_errors_parsing_manifest(self):
        archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0",
            "11/12/2014",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0.tar.gz"
        )
        self.syncer.sync_archive(archive)
        self.assertEqual(self.pg_driver.node_lookup(label="file").count(), 109)
        self.assertEqual(self.pg_driver.node_lookup(label="file").first().system_annotations["md5_source"],
                         "gdc_import_process")
        self.assertEqual(self.pg_driver.node_lookup(label="archive").count(), 1)

    def test_replacing_old_archive_works(self):
        old_archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.1.0",
            "somedate",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.1.0.tar.gz")
        new_archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0",
            "11/12/2014",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0.tar.gz")

        self.syncer.sync_archive(old_archive)
        self.syncer.sync_archive(new_archive)
        self.assertEqual(self.pg_driver.node_lookup(label="file").count(), 109)
        self.assertEqual(self.pg_driver.node_lookup(label="archive").count(), 1)
