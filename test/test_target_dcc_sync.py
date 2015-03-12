from unittest import TestCase
from multiprocessing import Process
import random
import time
import tempfile

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver
from signpost import Signpost

from psqlgraph import PsqlGraphDriver
from signpostclient import SignpostClient

from zug.datamodel.prelude import create_prelude_nodes
from zug.datamodel.target.dcc_sync import TARGETDCCProjectSyncer

from httmock import urlmatch, HTTMock
from mock import patch


def run_signpost(port):
    Signpost({"driver": "inmemory", "layers": ["validator"]}).run(host="localhost",
                                                                  port=port)


@urlmatch(netloc='target-data.nci.nih.gov')
def target_file_mock(url, request):
    content = 'this is some fake test content for this file'
    return {'content': content,
            'headers': {'Content-Length': str(len(content))}}


def fake_tree_walk(url, **kwargs):
    for url in ["https://target-data.nci.nih.gov/WT/Discovery/WXS/L3/mutation/BCM/target-wt-snp-indel.mafplus.txt"]:
        yield url


class TARGETDCCSyncTest(TestCase):

    # TODO mostly copy paste job from TCGA DCC tests, figure out how
    # to share code between tests better
    @classmethod
    def setUpClass(cls):
        cls.port = random.randint(5000, 6000)
        cls.signpost = Process(target=run_signpost, args=[cls.port])
        cls.signpost.start()
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        cls.signpost.terminate()

    def setUp(self):
        self.graph = PsqlGraphDriver('localhost', 'test',
                                     'test', 'automated_test')
        self.graph_info = {
            "host": "localhost",
            "user": "test",
            "pass": "test",
            "database": "automated_test"
        }
        self.scratch_dir = tempfile.mkdtemp()
        Local = get_driver(Provider.LOCAL)
        self.storage_client = Local(self.scratch_dir)
        self.storage_client.create_container("target_dcc_protected")
        self.storage_info = {
            "driver": Local,
            "access_key": self.scratch_dir,
            "kwargs": {}
        }
        self.signpost_url = "http://localhost:{}".format(self.port)
        self.signpost_client = SignpostClient(self.signpost_url,
                                              version="v0")
        create_prelude_nodes(self.graph)

    def tearDown(self):
        with self.graph.engine.begin() as conn:
            conn.execute('delete from edges')
            conn.execute('delete from nodes')
            conn.execute('delete from voided_edges')
            conn.execute('delete from voided_nodes')
        self.graph.engine.dispose()
        for container in self.storage_client.list_containers():
            for obj in container.list_objects():
                obj.delete()

    @patch("zug.datamodel.target.dcc_sync.tree_walk", fake_tree_walk)
    def test_basic_sync(self):
        syncer = TARGETDCCProjectSyncer(
            "WT",
            signpost_url=self.signpost_url,
            graph_info=self.graph_info,
            storage_info=self.storage_info,
        )
        with HTTMock(target_file_mock):
            syncer.sync()
            with self.graph.session_scope():
                file = self.graph.nodes()\
                                 .labels("file")\
                                 .sysan({"source": "target_dcc"}).one()
                subtype = self.graph.nodes().labels("data_subtype").with_edge_from_node("member_of", file).one()
            self.assertEqual(file["file_name"], "target-wt-snp-indel.mafplus.txt")
            self.assertEqual(file.acl, ["phs000218", "phs000471"])
            self.assertEqual(file["md5sum"], '5a7146f821d11c8fa91a0f5865f7b6f8')
            self.assertEqual(subtype["name"], "Simple somatic mutation")
