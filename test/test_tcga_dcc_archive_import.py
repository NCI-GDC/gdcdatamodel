from unittest import TestCase
from mock import patch

import tempfile

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

from zug.datamodel.tcga_dcc_sync import TCGADCCArchiveSyncer
from zug.datamodel.latest_urls import LatestURLParser

from zug.datamodel.prelude import create_prelude_nodes

from psqlgraph import PsqlGraphDriver


class TCGADCCArchiveSyncTest(TestCase):

    def setUp(self):
        self.pg_driver = PsqlGraphDriver('localhost', 'test',
                                         'test', 'automated_test')
        self.scratch_dir = tempfile.mkdtemp()
        Local = get_driver(Provider.LOCAL)
        self.storage_client = Local(tempfile.mkdtemp())
        self.storage_client.create_container("tcga_dcc_public")
        self.storage_client.create_container("tcga_dcc_protected")
        self.signpost_url = "http://localhost:5000"
        self.parser = LatestURLParser()
        create_prelude_nodes(self.pg_driver)

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

    def syncer_for(self, archive, **kwargs):
        return TCGADCCArchiveSyncer(
            archive,
            signpost_url=self.signpost_url,
            pg_driver=self.pg_driver,
            scratch_dir=self.scratch_dir,
            storage_client=self.storage_client,
            **kwargs
        )

    def test_syncing_an_archive_works(self):
        archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0",
            "11/12/2014",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0.tar.gz"
        )
        syncer = self.syncer_for(archive, download=False)
        syncer.sync()
        self.assertEqual(self.pg_driver.node_lookup(label="file").count(), 106)
        self.assertEqual(self.pg_driver.node_lookup(label="archive").count(), 1)
        archive = self.pg_driver.node_lookup(label="archive").one()
        file = self.pg_driver.node_lookup(label="file").first()
        # make sure archive gets tied to project
        self.pg_driver.node_lookup(label="project", property_matches={"name": "PAAD"})\
                      .with_edge_from_node("member_of", archive).one()
        # make sure the files get tied to classification stuff
        self.pg_driver.node_lookup(label="data_subtype").with_edge_from_node("member_of", file).one()

    def test_syncing_is_idempotent(self):
        archive = self.parser.parse_archive(
            "mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0",
            "11/12/2014",
            "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/paad/cgcc/mdanderson.org/mda_rppa_core/protein_exp/mdanderson.org_PAAD.MDA_RPPA_Core.Level_3.1.2.0.tar.gz"
        )
        syncer = self.syncer_for(archive, download=False)
        syncer.sync()
        syncer.sync()
        self.assertEqual(self.pg_driver.node_lookup(label="file").count(), 106)
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

        syncer = self.syncer_for(old_archive, download=False)
        syncer.sync()
        syncer = self.syncer_for(new_archive, download=False)
        syncer.sync()

        self.assertEqual(self.pg_driver.node_lookup(label="file").count(), 106)
        self.assertEqual(self.pg_driver.node_lookup(label="archive").count(), 1)
