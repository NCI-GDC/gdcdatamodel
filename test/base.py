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
from cdisutils.log import get_logger

from psqlgraph import PsqlGraphDriver, Node, Edge
from zug.datamodel.prelude import create_prelude_nodes
from signpost import Signpost
from signpostclient import SignpostClient

from moto import mock_s3bucket_path
import boto
from boto.s3.connection import OrdinaryCallingFormat

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
PG_HOST = 'localhost'
PG_USER = 'test'
PG_PASSWORD = 'test'
PG_DATABASE = 'automated_test'


def run_signpost(port):
    Signpost({"driver": "inmemory", "layers": ["validator"]}).run(
        host="localhost", port=port)


class ZugTestBase(TestCase):

    def setUp(self):
        self.basic_test_setup()
        self.delete_non_prelude_nodes()

    def tearDown(self):
        self.delete_non_prelude_nodes()
        self.g.engine.dispose()

    def delete_all_nodes(self):
        self._delete_all_nodes(self.g)

    @staticmethod
    def _delete_all_nodes(g):
        tables = [t for l in map(lambda x: x().get_subclass_table_names(),
                                 (Edge, Node)) for t in l
                  if t != Edge.__tablename__ and t != Node.__tablename__]
        tables += ['_voided_nodes', '_voided_edges']
        with g.engine.begin() as conn:
            conn.execute('TRUNCATE {}'.format(', '.join(tables)))

    def delete_non_prelude_nodes(self):
        self.log.info('Deleting all non-prelude nodes')
        with self.g.session_scope():
            for scls in Node.get_subclasses():
                self.g.nodes(scls).not_sysan(is_prelude=True).delete(
                    synchronize_session='fetch')
        with self.g.engine.begin() as conn:
            conn.execute('TRUNCATE _voided_nodes, _voided_edges')

    def basic_test_setup(self):
        self.graph_info = {
            "host": PG_HOST,
            "user": PG_USER,
            "password": PG_PASSWORD,
            "database": PG_DATABASE,
        }
        self.create_new_graph_driver()
        self.create_new_scratch_space()
        self.set_database_environ_variables()

    @classmethod
    def setUpClass(cls):
        cls.log = get_logger(cls.__name__)
        g = PsqlGraphDriver(PG_HOST, PG_USER, PG_PASSWORD, PG_DATABASE)
        cls.log.info('Deleting all nodes')
        cls._delete_all_nodes(g)

    @classmethod
    def tearDownClass(cls):
        g = PsqlGraphDriver(PG_HOST, PG_USER, PG_PASSWORD, PG_DATABASE)
        cls.log.info('Deleting all nodes')
        cls._delete_all_nodes(g)

    def set_database_environ_variables(self):
        os.environ["PG_HOST"] = self.graph_info['host']
        os.environ["PG_USER"] = self.graph_info['user']
        os.environ["PG_PASS"] = self.graph_info['password']
        os.environ["PG_NAME"] = self.graph_info['database']

    def create_new_graph_driver(self):
        self.graph = PsqlGraphDriver(**self.graph_info)
        self.g = self.graph

    def create_new_scratch_space(self):
        self.scratch_dir = tempfile.mkdtemp()

    @classmethod
    def random_string(cls, length=6):
        return ''.join([random.choice(string.ascii_lowercase + string.digits)
                        for _ in range(length)])

    @classmethod
    def get_fuzzed_node(cls, node_class, node_id=None, **kwargs):
        if node_id is None:
            node_id = str(uuid.uuid4())
        for key, types in node_class.get_pg_properties().iteritems():
            if key in kwargs:
                continue
            elif not types or str in types:
                kwargs[key] = cls.random_string()
            elif int in types or long in types:
                kwargs[key] = random.randint(1e6, 1e7)
            elif float in types:
                kwargs[key] = random.random()
            elif bool in types:
                kwargs[key] = random.choice((True, False))
        return node_class(node_id, **kwargs)


class SignpostMixin(object):

    @classmethod
    def setUpClass(cls):
        super(SignpostMixin, cls).setUpClass()
        cls.port = random.randint(5000, 6000)
        cls.log.info('Starting signpost')
        cls.signpost = Process(target=run_signpost, args=[cls.port])
        cls.signpost.start()
        time.sleep(1)
        cls.signpost_url = "http://localhost:{}".format(cls.port)
        cls.signpost_client = SignpostClient(cls.signpost_url, version="v0")

    @classmethod
    def tearDownClass(self):
        super(SignpostMixin, self).tearDownClass()
        self.log.info('Stopping signpost')
        self.signpost.terminate()


class PreludeMixin(object):

    @classmethod
    def setUpClass(cls):
        super(PreludeMixin, cls).setUpClass()
        g = PsqlGraphDriver(PG_HOST, PG_USER, PG_PASSWORD, PG_DATABASE)
        cls.log.info('Creating prelude nodes')
        create_prelude_nodes(g)
        with g.session_scope():
            for node in g.nodes().all():
                node.sysan['is_prelude'] = True

    @classmethod
    def tearDownClass(self):
        super(PreludeMixin, self).tearDownClass()


class StorageMixin(object):

    def setUp(self):
        super(StorageMixin, self).setUp()
        self.log.info('Creating storage client')
        Local = get_driver(Provider.LOCAL)
        self.storage_client = Local(self.scratch_dir)
        self.storage_info = {
            "driver": Local,
            "access_key": self.scratch_dir,
            "kwargs": {}
        }
        self.signpost_client = SignpostClient(self.signpost_url, version="v0")

    def tearDown(self):
        super(StorageMixin, self).tearDown()


class FakeS3Mixin(object):

    def with_fake_s3(self, f):
        def wrapper(*args, **kwargs):
            self.fake_s3.start()
            try:
                return f(*args, **kwargs)
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
