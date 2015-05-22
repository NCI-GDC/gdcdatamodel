from consulate import Consul
from moto import mock_s3bucket_path
from gdcdatamodel.models import File
from boto.s3.connection import OrdinaryCallingFormat
from base import ZugsTestBase
from zug.consul_mixin import ConsulMixin
from cdisutils.log import get_logger
from contextlib import contextmanager
from zug.backup import DataBackup
import uuid
from boto.s3.key import Key
import boto
from zug.downloaders import md5sum_with_size

class DataBackupTest(ZugsTestBase):
    def setUp(self):
        super(DataBackupTest, self).setUp()
        self.consul = Consul()
        assert self.consul.catalog.datacenters() == 'dc1'
        self.consul.kv.set("databackup/signpost_url", self.signpost_url)
        self.consul.kv.set("databackup/s3/s3.amazonaws.com/port", "80")
        self.consul.kv.set("databackup/s3/s3.amazonaws.com/access_key", "fake_access_key")
        self.consul.kv.set("databackup/s3/s3.amazonaws.com/secret_key", "fake_secret_key")
        self.consul.kv.set("databackup/ds3/test_backup/host", "s3.amazonaws.com")
        self.consul.kv.set("databackup/ds3/test_backup/port", "80")
        self.consul.kv.set("databackup/ds3/test_backup/access_key", "fake_access_key")
        self.consul.kv.set("databackup/ds3/test_backup/secret_key", "fake_secret_key")
        self.consul.kv.set("databackup/pg/host", "localhost")
        self.consul.kv.set("databackup/pg/user", "test")
        self.consul.kv.set("databackup/pg/pass", "test")
        self.consul.kv.set("databackup/pg/name", "automated_test")
        self.setup_fake_s3()
        self.setup_fake_files()
        DataBackup.BACKUP_DRIVER = ['test_backup']


    def tearDown(self):
        super(DataBackupTest, self).tearDown()
        self.consul.kv.delete("databackup/", recurse=True)
        self.delete_blackpearl_bucket()


    def delete_blackpearl_bucket(self):
        with self.with_fake_s3():
            conn = boto.connect_s3(calling_format=OrdinaryCallingFormat())
            if conn.lookup('ds3_fake_cghub_protected'):
                bucket = conn.get_bucket('ds3_fake_cghub_protected')
                for key in bucket.list():
                    bucket.delete_key(key.name)
                conn.delete_bucket(bucket.name)
                    

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

    def get_boto_key(self, name):
        with self.with_fake_s3():
            conn = boto.connect_s3(calling_format=OrdinaryCallingFormat())
            bucket = conn.get_bucket('ds3_fake_cghub_protected')
            return bucket.get_key(name)

    def test_single_file_backup(self):
        backup = DataBackup(file_id=self.file1.node_id,
                            debug=True,
                            bucket_prefix='ds3')
        with self.with_fake_s3():
            backup.backup()
            uploaded = self.get_boto_key(self.file1.file_name)
        self.assertEqual(self.file1.file_size, uploaded.size)
        with self.graph.session_scope():
            node = self.graph.nodes(File).ids(self.file1.node_id).one()
            self.assertEqual(node.system_annotations['test_backup'],'backuped')

    def test_backup_with_all_files_backuped(self):
        with self.graph.session_scope() as s:
            self.file1.system_annotations['test_backup']='backuped'
            self.file2.system_annotations['test_backup']='backuped'
            s.add(self.file1)
            s.add(self.file2)
            s.commit()
        backup = DataBackup(debug=True, bucket_prefix='ds3')
        with self.with_fake_s3():
            backup.backup()

    def test_backup_file_whose_verification_failed(self):
        with self.graph.session_scope() as s:
            self.file1.system_annotations['test_backup']='backuped'
            self.file2.system_annotations['test_backup']='failed'
            self.file1 = s.merge(self.file1)
            self.file2 = s.merge(self.file2)
            s.commit()
        backup = DataBackup(debug=True,
                            bucket_prefix='ds3')
        with self.with_fake_s3():
            backup.backup()
            uploaded = self.get_boto_key(self.file2.file_name)
        self.assertEqual(self.file2.file_size, uploaded.size)
        with self.graph.session_scope():
            node = self.graph.nodes(File).ids(self.file2.node_id).one()
            self.assertEqual(node.system_annotations['test_backup'],'backuped')


    def test_backup_random_file(self):
        backup = DataBackup(debug=True,
                            bucket_prefix='ds3')
        with self.with_fake_s3():
            backup.backup()
    
    def test_multiple_file_backup(self):
        backup = DataBackup(file_id=self.file1.node_id,
                            debug=True,
                            bucket_prefix='ds3')
        backup2 = DataBackup(file_id=self.file2.node_id,
                             debug=True,
                             bucket_prefix='ds3')

        with self.with_fake_s3():
            backup.backup()
            backup2.backup()
            uploaded = self.get_boto_key(self.file1.file_name)
            uploaded2 = self.get_boto_key(self.file2.file_name)
        self.assertEqual(self.file1.file_size, uploaded.size)
        self.assertEqual(self.file2.file_size, uploaded2.size)


    def test_backup_locked_file(self):
        consul = ConsulMixin(prefix='databackup')
        consul.key = self.file1.node_id
        consul.start_consul_session()
        consul.get_consul_lock()
        backup = DataBackup(file_id=self.file1.node_id,
                            debug=True,
                            bucket_prefix='ds3')
        with self.with_fake_s3():
            backup.backup()
            conn = boto.connect_s3(calling_format=OrdinaryCallingFormat())
            self.assertIsNone(conn.lookup('ds3_fake_cghub_protected'))

    def setup_fake_s3(self):
        self.fake_s3 = mock_s3bucket_path()
        for backend in self.fake_s3.backends.values():
            # lololol TODO write explaination for this nonsense
            backend.reset = lambda: None
        self.fake_s3.start()
        conn = boto.connect_s3(calling_format=OrdinaryCallingFormat())
        conn.create_bucket("fake_cghub_protected")
        self.fake_s3.stop()
