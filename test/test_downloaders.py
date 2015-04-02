import os
import uuid
import boto
from base import ZugsTestBase
from mock import patch, Mock
from moto import mock_s3bucket_path

from boto.s3.connection import OrdinaryCallingFormat

from zug.downloaders import Downloader, md5sum


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


class DownloadersTest(ZugsTestBase):

    def setUp(self):
        super(DownloadersTest, self).setUp()
        self.gtdownload_dict = {}

    @property
    def fake_consul_data(self):
        return {
            "downloaders": {
                "signpost_url": self.signpost_url,
                "path": self.scratch_dir,
                "cghub_key": "fake_cghub_key",
                "pg": {
                    "host": "localhost",
                    "user": "test",
                    "pass": "test",
                    "name": "automated_test",
                },
                "s3": {
                    "host": "s3.amazonaws.com",
                    "port": "80",
                    "access_key": "fake_access",
                    "secret_key": "fake_secret",
                    "buckets": {
                        "fake_cghub": "fake_cghub_protected"
                    }
                }
            }
        }

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
                "md5sum": md5sum(content),
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

    @mock_s3bucket_path
    @patch("zug.downloaders.Downloader.check_gtdownload", lambda self: None)
    @patch("zug.downloaders.Pool", FakePool)
    @patch("zug.downloaders.Downloader.get_free_space", lambda self: 1000000000000)
    def test_basic_download(self):
        conn = boto.connect_s3(calling_format=OrdinaryCallingFormat())
        conn.create_bucket("fake_cghub_protected")
        aid = str(uuid.uuid4())
        self.files = []
        self.files.append(self.create_file("foobar.bam", "fake bam test content", aid))
        self.files.append(self.create_file("foobar.bam.bai", "fake bai test content", aid))
        def mock_consul_get(consul, path):
            return get_in(self.fake_consul_data, path)
        with patch("zug.downloaders.consul_get", mock_consul_get), patch("zug.downloaders.Downloader.call_gtdownload", self.call_gtdownload):
            downloader = Downloader(source="fake_cghub")
            downloader.go()
        with self.graph.session_scope():
            for file in self.graph.nodes().labels("file").sysan({"analysis_id": aid}).all():
                self.assertEqual(file["state"], "live")
                url = self.signpost_client.get(file.node_id).urls[0]
                expected_url = "s3://s3.amazonaws.com/fake_cghub_protected/{}/{}".format(file.system_annotations["analysis_id"],
                                                                                         file["file_name"])
                self.assertEqual(expected_url, url)
                assert file.system_annotations["import_took"] > 0
                assert file.system_annotations["import_completed"] > file.system_annotations["import_took"]

    @mock_s3bucket_path
    @patch("zug.downloaders.Downloader.check_gtdownload", lambda self: None)
    @patch("zug.downloaders.Pool", FakePool)
    @patch("zug.downloaders.Downloader.get_free_space", lambda self: 1000000000000)
    def test_specifying_analysis_id(self):
        conn = boto.connect_s3(calling_format=OrdinaryCallingFormat())
        conn.create_bucket("fake_cghub_protected")
        aid = str(uuid.uuid4())
        self.files = []
        self.files.append(self.create_file("foobar.bam", "fake bam test content", aid))
        self.files.append(self.create_file("foobar.bam.bai", "fake bai test content", aid))
        def mock_consul_get(consul, path):
            return get_in(self.fake_consul_data, path)
        with patch("zug.downloaders.consul_get", mock_consul_get), patch("zug.downloaders.Downloader.call_gtdownload", self.call_gtdownload):
            downloader = Downloader(source="fake_cghub", analysis_id=aid)
            downloader.go()
        with self.graph.session_scope():
            for file in self.graph.nodes().labels("file").sysan({"analysis_id": aid}).all():
                self.assertEqual(file["state"], "live")
                url = self.signpost_client.get(file.node_id).urls[0]
                expected_url = "s3://s3.amazonaws.com/fake_cghub_protected/{}/{}".format(file.system_annotations["analysis_id"],
                                                                                         file["file_name"])
                self.assertEqual(expected_url, url)

    @mock_s3bucket_path
    @patch("zug.downloaders.Downloader.check_gtdownload", lambda self: None)
    @patch("zug.downloaders.Pool", FakePool)
    @patch("zug.downloaders.Downloader.get_free_space", lambda self: 1000000000000)
    def test_file_is_invalidated_if_checksum_fails(self):
        conn = boto.connect_s3(calling_format=OrdinaryCallingFormat())
        conn.create_bucket("fake_cghub_protected")
        aid = str(uuid.uuid4())
        self.files = []
        bam_file = self.create_file("foobar.bam", "fake bam test content", aid)
        self.files.append(bam_file)
        self.graph.node_update(bam_file, properties={"md5sum": "bogus"})
        self.files.append(self.create_file("foobar.bam.bai", "fake bai test content", aid))
        def mock_consul_get(consul, path):
            return get_in(self.fake_consul_data, path)
        with patch("zug.downloaders.consul_get", mock_consul_get), patch("zug.downloaders.Downloader.call_gtdownload", self.call_gtdownload):
            downloader = Downloader(source="fake_cghub", analysis_id=aid)
            downloader.go()
        with self.graph.session_scope():
            bam_file = self.graph.nodes().labels("file").props({"file_name": "foobar.bam"}).one()
        self.assertEqual(bam_file["state"], "invalid")
