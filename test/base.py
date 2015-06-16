from unittest import TestCase

import os
import random
import tempfile
import string
import time
import uuid
from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver
from multiprocessing import Process

from psqlgraph import PsqlGraphDriver, Node, Edge
from zug.datamodel.prelude import create_prelude_nodes
from signpost import Signpost
from signpostclient import SignpostClient

from moto import mock_s3bucket_path
import boto
from boto.s3.connection import OrdinaryCallingFormat

TEST_DIR = os.path.dirname(os.path.realpath(__file__))


def run_signpost(port):
    Signpost({"driver": "inmemory", "layers": ["validator"]}).run(
        host="localhost", port=port)


class ZugsTestBase(TestCase):

    def random_string(self, length=6):
        return ''.join([random.choice(string.ascii_lowercase + string.digits)
                        for _ in range(length)])

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
        self.graph_info = {
            "host": "localhost",
            "user": "test",
            "password": "test",
            "database": "automated_test"
        }
        self.graph = PsqlGraphDriver(**self.graph_info)
        self.graph_info['pass'] = self.graph_info['password']
        self.scratch_dir = tempfile.mkdtemp()
        Local = get_driver(Provider.LOCAL)
        self.storage_client = Local(self.scratch_dir)
        self.storage_info = {
            "driver": Local,
            "access_key": self.scratch_dir,
            "kwargs": {}
        }
        self.signpost_url = "http://localhost:{}".format(self.port)
        self.signpost_client = SignpostClient(self.signpost_url, version="v0")
        create_prelude_nodes(self.graph)

    def tearDown(self):
        with self.graph.engine.begin() as conn:
            for table in Node().get_subclass_table_names():
                if table != Node.__tablename__:
                    conn.execute('delete from {}'.format(table))
            for table in Edge().get_subclass_table_names():
                if table != Edge.__tablename__:
                    conn.execute('delete from {}'.format(table))
            conn.execute('delete from _voided_nodes')
            conn.execute('delete from _voided_edges')
        self.graph.engine.dispose()

    def get_fuzzed_node(self, cls, node_id=None, **kwargs):
        if node_id is None:
            node_id = str(uuid.uuid4())
        for key, types in cls.get_pg_properties().iteritems():
            if key in kwargs:
                continue
            elif not types or str in types:
                kwargs[key] = self.random_string()
            elif int in types or long in types:
                kwargs[key] = random.randint(1e6, 1e7)
            elif float in types:
                kwargs[key] = random.random()
            elif bool in types:
                kwargs[key] = random.choice((True, False))
        return cls(node_id, **kwargs)


class FakeS3Mixin(object):

    def with_fake_s3(self, f):
        def wrapper(*args, **kwargs):
            self.fake_s3.start()
            try:
                f(*args, **kwargs)
            finally:
                self.fake_s3.stop()
        return wrapper

    def setup_fake_s3(self, bucket_name):
        self.fake_s3 = mock_s3bucket_path()
        for backend in self.fake_s3.backends.values():
            # lololol TODO write explaination for this nonsense
            backend.reset = lambda: None
        self.fake_s3.start()
        conn = boto.connect_s3(calling_format=OrdinaryCallingFormat())
        conn.create_bucket(bucket_name)
        self.fake_s3.stop()
