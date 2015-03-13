from unittest import TestCase

import os
import random
import tempfile
import time
from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver
from multiprocessing import Process

from psqlgraph import PsqlGraphDriver
from zug.datamodel.prelude import create_prelude_nodes
from signpost import Signpost
from signpostclient import SignpostClient


TEST_DIR = os.path.dirname(os.path.realpath(__file__))


def run_signpost(port):
    Signpost({"driver": "inmemory", "layers": ["validator"]}).run(host="localhost",
                                                                  port=port)


class ZugsTestBase(TestCase):

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
