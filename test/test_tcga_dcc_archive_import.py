from unittest import TestCase

import tempfile

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

from zug.datamodel.tcga_dcc_sync import TCGADCCArchiveSyncer

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

    def tearDown(self):
        with self.pg_driver.session_scope() as session:
            for node in self.pg_driver.get_nodes(session):
                self.pg_driver.node_delete(node=node, session=session)
            for edge in self.pg_driver.get_edges(session):
                self.pg_driver.edge_delete(edge=edge, session=session)
        for container in self.storage_client.list_containers():
            for obj in container.list_objects():
                obj.delete()

    def test_syncing_an_archive_works(self):
        archive = {
            '_type': 'tcga_dcc_archive',
            'archive_name': u'bcgsc.ca_ACC.IlluminaHiSeq_miRNASeq.Level_3.1.1.0',
            'batch': 1,
            'center_name': u'bcgsc.ca',
            'center_type': u'cgcc',
            'data_level': u'Level_3',
            'data_type_in_url': u'mirnaseq',
            'date_added': u'10/03/2013 17:06',
            'dcc_archive_url': u'https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/acc/cgcc/bcgsc.ca/illuminahiseq_mirnaseq/mirnaseq/bcgsc.ca_ACC.IlluminaHiSeq_miRNASeq.Level_3.1.1.0.tar.gz',
            'disease_code': u'ACC',
            'import': {'download': {'finish_time': None, 'start_time': None},
                       'finish_time': None,
                       'host': None,
                       'process': {'finish_time': None, 'start_time': None},
                       'start_time': None,
                       'state': 'not_started',
                       'upload': {'finish_time': None, 'start_time': None}},
            'platform': u'IlluminaHiSeq_miRNASeq',
            'platform_in_url': u'illuminahiseq_mirnaseq',
            'protected': False,
            'revision': 1,
            'signpost_added': '2015-01-05T23:32:06+00:00'
        }
        self.syncer.sync_archive(archive)
        self.assertEqual(self.pg_driver.node_lookup(label="file").count(), 163)
        self.assertEqual(self.pg_driver.node_lookup(label="archive").count(), 1)
