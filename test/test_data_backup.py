from consulate import Consul
from moto import mock_s3bucket_path
from gdcdatamodel.models import File
from boto.s3.connection import OrdinaryCallingFormat
from base import ZugsTestBase
from contextlib import contextmanager
from zug.backup import DataBackup
import uuid
from boto.s3.key import Key
import os
import boto

class DataBackupTest(ZugsTestBase):
    def setUp(self):
        super(DataBackupTest, self).setUp()
        self.consul = Consul()
        assert self.consul.catalog.datacenters() == 'dc1'
        self.consul.kv.set("databackup/signpost_url", self.signpost_url)
        self.consul.kv.set("databackup/path", self.scratch_dir)
        self.consul.kv.set("databackup/s3/host", "s3.amazonaws.com")
        self.consul.kv.set("databackup/s3/port", "80")
        self.consul.kv.set("databackup/s3/access_key", "fake_access_key")
        self.consul.kv.set("databackup/s3/secret_key", "fake_secret_key")
        self.consul.kv.set("databackup/ds3/host", "ds3.amazonaws.com")
        self.consul.kv.set("databackup/ds3/port", "80")
        self.consul.kv.set("databackup/ds3/access_key", "fake_access_key")
        self.consul.kv.set("databackup/ds3/secret_key", "fake_secret_key")

        self.consul.kv.set("databackup/s3/buckets/fake_cghub",
                           "fake_cghub_protected")
        self.consul.kv.set("databackup/pg/host", "localhost")
        self.consul.kv.set("databackup/pg/user", "test")
        self.consul.kv.set("databackup/pg/pass", "test")
        self.consul.kv.set("databackup/pg/name", "automated_test")
        self.setup_fake_s3()
        self.setup_fake_files()

    def tearDown(self):
        super(DataBackupTest, self).tearDown()
        self.consul.kv.delete("databackup/", recurse=True)

    def create_file(self, name, content, aid, session):
        doc = self.signpost_client.create()
        doc.urls=["s3://localhost/fake_cghub_protected/{}".format(name)]
        doc.patch()
        file = File(
            node_id=doc.did,
            properties={
                "file_name": name,
                "md5sum": 'test',
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
            self.create_file("foobar.bam", "fake bam test content",
                             self.aid, session)
            self.create_file("foobar.bam.bai", "fake bai test content",
                             self.aid, session)
        self.fake_s3.start()
        conn = boto.connect_s3(calling_format=OrdinaryCallingFormat())
        bucket = conn.get_bucket("fake_cghub_protected")
        k = Key(bucket)
        k.key = 'foobar.bam'
        k.set_contents_from_string("fake bam test content")
        k2 = Key(bucket)
        k2.key = 'foobar.bam.bai'
        k2.set_contents_from_string("fake bai test content")
        self.fake_s3.stop()
    @contextmanager
    def with_fake_s3(self):
        self.fake_s3.start()
        yield
        self.fake_s3.stop()

    def test_download(self):
        backup = DataBackup(clear=False, debug=True)
        with backup.consul_session_scope(),\
                self.with_fake_s3():
            backup.get_file_to_backup()
            backup.download()
        downloaded = os.path.join(backup.download_path, backup.file_id)
        self.assertEqual(os.path.getsize(downloaded), backup.file.file_size)

    def setup_fake_s3(self):
        self.fake_s3 = mock_s3bucket_path()
        for backend in self.fake_s3.backends.values():
            # lololol TODO write explaination for this nonsense
            backend.reset = lambda: None
        self.fake_s3.start()
        conn = boto.connect_s3(calling_format=OrdinaryCallingFormat())
        conn.create_bucket("fake_cghub_protected")
        self.fake_s3.stop()


