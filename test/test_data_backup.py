from consulate import Consul
from multiprocessing import Process
import time
from moto import mock_s3bucket_path
from gdcdatamodel.models import File
from boto.s3.connection import OrdinaryCallingFormat
from base import ZugTestBase
from contextlib import contextmanager
from zug import backup
import uuid
import contextlib
from boto.s3.key import Key
import boto
import os
from ds3client import client, mock
from zug.downloaders import md5sum_with_size
from test_downloaders import FakePool
from mock import patch
from cdisutils.net import BotoManager

def run_ds3(port):
    return mock.app.run(host='localhost', port=port)


class DataBackupTest(ZugTestBase):
    @classmethod
    def setUpClass(cls):
        super(DataBackupTest, cls).setUpClass()
        cls.ds3 = Process(target=run_ds3, args=[cls.port+1])
        cls.ds3.start()
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        super(DataBackupTest, cls).tearDownClass()
        cls.ds3.terminate()

    def setUp(self):
        super(DataBackupTest, self).setUp()
        self.consul = Consul()
        assert self.consul.catalog.datacenters() == 'dc1'
        self.consul.kv.set("databackup/signpost_url", self.signpost_url)
        self.consul.kv.set("databackup/s3-endpoints", ["s3.amazonaws.com"])
        self.consul.kv.set("databackup/s3/s3.amazonaws.com/port", 80)
        self.consul.kv.set("databackup/s3/s3.amazonaws.com/access_key",
                           "fake_access_key")
        self.consul.kv.set("databackup/s3/s3.amazonaws.com/secret_key",
                           "fake_secret_key")
        self.consul.kv.set("databackup/ds3/test_backup/host", "localhost")
        self.consul.kv.set("databackup/ds3/test_backup/port", self.port+1)
        self.consul.kv.set("databackup/pg/host", "localhost")
        self.consul.kv.set("databackup/path", os.path.dirname(os.path.realpath(__file__)))
        self.consul.kv.set("databackup/processes", 1)
        self.consul.kv.set("databackup/pg/user", "test")
        self.consul.kv.set("databackup/pg/pass", "test")
        self.consul.kv.set("databackup/pg/name", "automated_test")
        self.setup_fake_s3()
        self.setup_fake_files()
        self.ds3 = client.Client(host='localhost', port=self.port+1, protocol='http',
                                 access_key='', secret_key='')


    def tearDown(self):
        super(DataBackupTest, self).tearDown()
        self.consul.kv.delete("databackup/", recurse=True)

        for job in self.ds3.jobs:
            job.delete()
        for key in self.ds3.keys:
            key.delete()
        for bucket in self.ds3.buckets:
            bucket.delete()

    def s3_patch(self):
        return patch.multiple('backup',
                              download_file=self.wrap_fake_s3(backup.download_file))


    def create_file(self, name, content, aid, session):
        doc = self.signpost_client.create()
        doc.urls = ["s3://s3.amazonaws.com/fake_cghub_protected/{}".format(name)]
        doc.patch()
        file = File(
            node_id=doc.did,
            properties={
                "file_name": name,
                "md5sum": md5sum_with_size(content)[0],
                "file_size": len(content),
                "state": "live",
                "state_comment": None,
                "submitter_id": aid
            },
            system_annotations={
                "source": "fake_cghub",
                "analysis_id": aid,
            }
        )
        return session.merge(file)

    def setup_fake_files(self):
        self.aid = str(uuid.uuid4())
        with self.graph.session_scope() as session:
            self.file1 = self.create_file("foobar.bam",
                                          "fake bam test content",
                                          self.aid, session)
            self.file2 = self.create_file("foobar.bam.bai",
                                          "fake bai test content",
                                          self.aid, session)
        with self.with_fake_s3():
            conn = boto.connect_s3(calling_format=OrdinaryCallingFormat())
            bucket = conn.get_bucket("fake_cghub_protected")
            k = Key(bucket)
            k.key = 'foobar.bam'
            k.set_contents_from_string("fake bam test content")
            k2 = Key(bucket)
            k2.key = 'foobar.bam.bai'
            k2.set_contents_from_string("fake bai test content")

    @contextmanager
    def with_fake_s3(self):
        self.fake_s3.start()
        yield
        self.fake_s3.stop()


    def wrap_fake_s3(self, f):
        def wrapper(*args, **kwargs):
            self.fake_s3.start()
            try:
                return f(*args, **kwargs)
            finally:
                self.fake_s3.stop()
        return wrapper

    def get_boto_key(self, name):
        with self.with_fake_s3():
            conn = boto.connect_s3(calling_format=OrdinaryCallingFormat())
            bucket = conn.get_bucket('fake_cghub_protected')
            return bucket.get_key(name)

    def backup_patch(self):
        '''
        the contextlib.nested will not handle all managers' exit  properly 
        if any context manager throws an exception during __exit__(), 
        this is only used for moto patch for convenience.
        '''
        return contextlib.nested(patch('zug.backup.download_file', new=self.wrap_fake_s3(backup.download_file)),\
            patch('zug.backup.DataBackup.get_key_size', new=self.wrap_fake_s3(backup.DataBackup.get_key_size)))

    @patch("zug.backup.Pool", FakePool)
    def test_file_backup(self):
        backup_process = backup.DataBackup(driver='test_backup', debug=True,
                            protocol='http')
        with self.backup_patch():
            backup_process.backup()
        with self.graph.session_scope():
            for node in self.graph.nodes(File).all():
                ds3_file = self.ds3.keys(node.node_id).one()
                with self.with_fake_s3():
                    s3_file = self.get_boto_key(node.file_name)
                    content = s3_file.get_contents_as_string()
                self.assertEqual(ds3_file.get_contents_to_string(), content)
                self.assertEqual(node.system_annotations['test_backup'], 'backuped')

    def test_backup_with_all_files_backuped(self):
        with self.graph.session_scope() as s:
            self.file1.system_annotations['test_backup']='backuped'
            self.file2.system_annotations['test_backup']='backuped'
            s.add(self.file1)
            s.add(self.file2)
            s.commit()
        backup_process = backup.DataBackup(debug=True, driver='test_backup', protocol='http')
        with self.backup_patch():
            backup_process.backup()

    def test_backup_file_whose_verification_failed(self):
        with self.graph.session_scope() as s:
            self.file1.system_annotations['test_backup']='backuped'
            self.file2.system_annotations['test_backup']='failed'
            self.file1 = s.merge(self.file1)
            self.file2 = s.merge(self.file2)
            s.commit()
        backup_process = backup.DataBackup(debug=True,driver='test_backup', protocol='http')
        with self.backup_patch():
            backup_process.backup()
        ds3_file = self.ds3.keys(self.file2.node_id).one()
        with self.with_fake_s3():
            s3_file = self.get_boto_key(self.file2.file_name)
            content = s3_file.get_contents_as_string()
        self.assertEqual(ds3_file.get_contents_to_string(), content)

        with self.graph.session_scope():
            node = self.graph.nodes(File).ids(self.file2.node_id).one()
            self.assertEqual(node.system_annotations['test_backup'],'backuped')


    def test_backup_with_md5_failed(self):
        with self.graph.session_scope() as s:
            self.file1.system_annotations['md5_verify_status']='failed'
            self.file2.system_annotations['md5_verify_status']='failed'
            self.file1 = s.merge(self.file1)
            self.file2 = s.merge(self.file2)
            s.commit()
        backup_process = backup.DataBackup(debug=True,driver='test_backup', protocol='http')
        with self.backup_patch():
            backup_process.backup()

        with self.graph.session_scope():
            nodes = self.graph.nodes(File).not_sysan({'test_backup': 'backuped'})
            self.assertEqual(nodes.count(), 2)

    def setup_fake_s3(self):
        self.fake_s3 = mock_s3bucket_path()
        for backend in self.fake_s3.backends.values():
            # lololol TODO write explaination for this nonsense
            backend.reset = lambda: None
        self.fake_s3.start()
        conn = boto.connect_s3(calling_format=OrdinaryCallingFormat())
        conn.create_bucket("fake_cghub_protected")
        self.fake_s3.stop()
