import unittest
import tempfile
import time
import random
from multiprocessing import Process

from psqlgraph import PsqlGraphDriver, PsqlNode

from signpost import Signpost
from signpostclient import SignpostClient

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver


from zug.datalocator import DataLocator


def run_signpost(port):
    Signpost({"driver": "inmemory", "layers": ["validator"]}).run(host="localhost",
                                                                  port=port)


class DataLocatorTest(unittest.TestCase):

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
        self.scratch_dir = tempfile.mkdtemp()
        Local = get_driver(Provider.LOCAL)
        self.storage_client = Local(tempfile.mkdtemp())
        self.storage_client.create_container("test")
        self.storage_client.connection.host = "local"
        self.signpost_client = SignpostClient("http://localhost:{}".format(self.port),
                                              version="v0")

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

    def test_data_locate(self):
        doc = self.signpost_client.create()
        self.graph.node_insert(node=PsqlNode(node_id=doc.did,
                                             label='file',
                                             properties={"file_name": "baz.txt"},
                                             system_annotations={"analysis_id": "abc123"}))
        cont = self.storage_client.get_container("test")
        self.storage_client.upload_object_via_stream("data", cont, "abc123/baz.txt")
        self.locator = DataLocator(storage_client=self.storage_client,
                                   graph=self.graph,
                                   signpost_client=self.signpost_client)
        self.locator.sync("test")
        doc.refresh()
        self.assertEqual(doc.urls, ["file://local/test/abc123/baz.txt"])
