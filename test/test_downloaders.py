import os
import uuid
import boto
import hashlib
from itertools import islice
from base import ZugsTestBase
from mock import patch, Mock
from moto import mock_s3bucket_path
from consulate import Consul

from boto.s3.connection import OrdinaryCallingFormat

from zug.downloaders import Downloader, md5sum_with_size


def get_in(dictionary, path):
    return reduce(lambda d, k: d[k], path, dictionary)


class FakePool(object):
    """
    A fake multiprocessing.Pool to keep everything in process so moto works
    """

    def __init__(*args, **kwargs):
        pass

    def map_async(self, f, args_list):
        for args in args_list:
            f(args)
        return Mock()

    def close(self):
        pass

    def join(self):
        pass


class FailingMD5SumWithSize(object):
    """Fail once, sending returning a truncated version of this boto key,
    then succeed later.
    """

    def __init__(self):
        self.truncate = True

    def __call__(self, key):
        if self.truncate:
            self.truncate = False
            length = int(key.size * 0.8)
            iterable = islice(key, length)
        else:
            length = key.size
            iterable = key
        md5 = hashlib.md5()
        for chunk in iterable:
            md5.update(chunk)
        return md5.hexdigest(), length


class DownloadersTest(ZugsTestBase):

    def setUp(self):
        super(DownloadersTest, self).setUp()
        self.gtdownload_dict = {}
        self.consul = Consul()
        # this is to prevent accidentally blowing away the values on a real server
        assert self.consul.catalog.datacenters() == "dc1"
        self.consul.kv.set("downloaders/signpost_url", self.signpost_url)
        self.consul.kv.set("downloaders/path", self.scratch_dir)
        self.consul.kv.set("downloaders/s3/host", "s3.amazonaws.com")  # this is necessary for moto to work
        self.consul.kv.set("downloaders/s3/port", "80")
        self.consul.kv.set("downloaders/s3/access_key", "fake_access_key")
        self.consul.kv.set("downloaders/s3/secret_key", "fake_secret_key")
        self.consul.kv.set("downloaders/s3/buckets/fake_cghub", "fake_cghub_protected")
        self.consul.kv.set("downloaders/pg/host", "localhost")
        self.consul.kv.set("downloaders/pg/user", "test")
        self.consul.kv.set("downloaders/pg/pass", "test")
        self.consul.kv.set("downloaders/pg/name", "automated_test")

    def tearDown(self):
        super(DownloadersTest, self).tearDown()
        self.consul.kv.delete("downloaders/", recurse=True)

    def call_gtdownload(self):
        if self.gtdownload_dict:
            os.makedirs(os.path.join(self.scratch_dir, self.gtdownload_dict.keys()[0].system_annotations["analysis_id"]))
            for file, contents in self.gtdownload_dict.iteritems():
                path = os.path.join(self.scratch_dir, file.system_annotations["analysis_id"], file["file_name"])
                with open(path, "w") as f:
                    f.write(contents)

    def create_file(self, name, content, aid):
        did = self.signpost_client.create().did
        file = self.graph.node_merge(
            node_id=did,
            label="file",
            properties={
                "file_name": name,
                "md5sum": md5sum_with_size(content)[0],
                "file_size": len(content),
                "state": "submitted",
                "state_comment": None,
                "submitter_id": aid
            },
            system_annotations={
                "source": "fake_cghub",
                "analysis_id": aid,
            }
        )
        self.gtdownload_dict[file] = content
        return file

    def with_fake_s3(self, f):
        def wrapper(*args, **kwargs):
            self.fake_s3.start()
            try:
                f(*args, **kwargs)
            finally:
                self.fake_s3.stop()
        return wrapper

    def setup_fake_s3(self):
        self.fake_s3 = mock_s3bucket_path()
        for backend in self.fake_s3.backends.values():
            # lololol TODO write explaination for this nonsense
            backend.reset = lambda: None
        self.fake_s3.start()
        conn = boto.connect_s3(calling_format=OrdinaryCallingFormat())
        conn.create_bucket("fake_cghub_protected")
        self.fake_s3.stop()

    def setup_fake_files(self):
        self.aid = str(uuid.uuid4())
        self.files = []
        self.files.append(self.create_file("foobar.bam", "fake bam test content", self.aid))
        self.files.append(self.create_file("foobar.bam.bai", "fake bai test content", self.aid))

    def downloader_monkey_patches(self):
        return patch.multiple("zug.downloaders.Downloader",
                              call_gtdownload=self.call_gtdownload,
                              setup_s3=self.with_fake_s3(Downloader.setup_s3),
                              upload=self.with_fake_s3(Downloader.upload),
                              verify=self.with_fake_s3(Downloader.verify),
                              check_gtdownload=lambda self: None,
                              get_free_space=lambda self: 100000000)

    @patch("zug.downloaders.Pool", FakePool)
    def test_basic_download(self):
        self.setup_fake_s3()
        self.setup_fake_files()
        with self.downloader_monkey_patches():
            downloader = Downloader(source="fake_cghub")
            downloader.go()
        with self.graph.session_scope():
            for file in self.graph.nodes().labels("file").sysan({"analysis_id": self.aid}).all():
                self.assertEqual(file["state"], "live")
                url = self.signpost_client.get(file.node_id).urls[0]
                expected_url = "s3://s3.amazonaws.com/fake_cghub_protected/{}/{}".format(file.system_annotations["analysis_id"],
                                                                                         file["file_name"])
                self.assertEqual(expected_url, url)
                assert file.system_annotations["import_took"] > 0
                assert file.system_annotations["import_completed"] > file.system_annotations["import_took"]

    @patch("zug.downloaders.Pool", FakePool)
    def test_specifying_analysis_id(self):
        self.setup_fake_s3()
        self.setup_fake_files()
        with self.downloader_monkey_patches():
            downloader = Downloader(source="fake_cghub", analysis_id=self.aid)
            downloader.go()
        with self.graph.session_scope():
            for file in self.graph.nodes().labels("file").sysan({"analysis_id": self.aid}).all():
                self.assertEqual(file["state"], "live")
                url = self.signpost_client.get(file.node_id).urls[0]
                expected_url = "s3://s3.amazonaws.com/fake_cghub_protected/{}/{}".format(file.system_annotations["analysis_id"],
                                                                                         file["file_name"])
                self.assertEqual(expected_url, url)

    @patch("zug.downloaders.Pool", FakePool)
    def test_file_is_invalidated_if_checksum_fails(self):
        self.setup_fake_s3()
        self.setup_fake_files()
        with self.graph.session_scope():
            bam_file = self.graph.nodes().labels("file").props({"file_name": "foobar.bam"}).one()
            self.graph.node_update(bam_file, properties={"md5sum": "bogus"})
        with self.downloader_monkey_patches():
            downloader = Downloader(source="fake_cghub")
            downloader.go()
        with self.graph.session_scope():
            bam_file = self.graph.nodes().labels("file").props({"file_name": "foobar.bam"}).one()
        self.assertEqual(bam_file["state"], "invalid")

    @patch("zug.downloaders.Pool", FakePool)
    def test_startup_fails_if_analysis_id_is_locked(self):
        self.setup_fake_s3()
        self.setup_fake_files()
        sess = self.consul.session.create(ttl="60s")  # we just won't heartbeat this so it'll go away in minute
        assert self.consul.kv.acquire_lock("downloaders/current/{}".format(self.aid), sess)
        with self.downloader_monkey_patches():
            downloader = Downloader(source="fake_cghub")
            with self.assertRaises(RuntimeError):
                downloader.go()

    @patch("zug.downloaders.Pool", FakePool)
    @patch("zug.downloaders.md5sum_with_size", FailingMD5SumWithSize())
    def test_startup_temporary_stream_of_incorrect_length_is_handled(self):
        self.setup_fake_s3()
        self.setup_fake_files()
        with self.downloader_monkey_patches():
            downloader = Downloader(source="fake_cghub")
            downloader.go()

    @patch("zug.downloaders.Pool", FakePool)
    def test_mixed_states_are_handled(self):
        self.setup_fake_s3()
        self.setup_fake_files()
        self.graph.node_update(
            self.files[1],
            properties={"state": "live"}
        )
        with self.downloader_monkey_patches():
            downloader = Downloader(source="fake_cghub")
            downloader.go()
        with self.graph.session_scope():
            for file in self.graph.nodes().labels("file").sysan({"analysis_id": self.aid}).all():
                self.assertEqual(file["state"], "live")

    def test_dont_download_to_delete_files(self):
        self.setup_fake_s3()
        self.setup_fake_files()
        self.graph.node_update(
            self.files[1],
            system_annotations={"to_delete": True}
        )
        with self.downloader_monkey_patches():
            downloader = Downloader(source="fake_cghub")
            with self.assertRaises(RuntimeError):
                downloader.get_files_to_download()
