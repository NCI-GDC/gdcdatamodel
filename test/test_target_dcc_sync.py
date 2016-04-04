from base import ZugTestBase, StorageMixin, SignpostMixin, PreludeMixin

from gdcdatamodel.models import File
from zug.datamodel.target.dcc_sync import TARGETDCCProjectSyncer

from httmock import urlmatch, HTTMock
from mock import patch

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

@urlmatch(netloc='target-data.nci.nih.gov')
def target_file_mock(url, request):
    content = 'this is some fake test content for this file'
    return {'content': content,
            'headers': {'Content-Length': str(len(content))}}


def fake_tree_walk(url, **kwargs):
    for url in ["https://target-data.nci.nih.gov/Public/WT/WXS/L3/mutation/BCM/target-wt-snp-indel.mafplus.txt"]:
        yield url


class TARGETDCCSyncTest(PreludeMixin, StorageMixin,
                        SignpostMixin, ZugTestBase):

    def setUp(self):
        super(TARGETDCCSyncTest, self).setUp()
        Local = get_driver(Provider.LOCAL)
        self.bucket_name = "target_dcc_protected"
        self.storage_client.create_container(self.bucket_name)
        self.storage_info = {
            'driver': Local,
            'bucket': self.bucket_name,
            'access_key': self.scratch_dir,
            'kwargs': {}
        }

    @patch("zug.datamodel.target.dcc_sync.tree_walk", fake_tree_walk)
    def test_basic_sync(self):
        self.graph_info['pass'] = self.graph_info['password']
        syncer = TARGETDCCProjectSyncer(
            "WT",
            signpost_url=self.signpost_url,
            graph_info=self.graph_info,
            storage_info=self.storage_info,
        )
        with HTTMock(target_file_mock):
            syncer.sync()
            with self.graph.session_scope():
                file = self.graph.nodes(File)\
                                 .sysan({"source": "target_dcc"}).one()
                subtype = file.data_subtypes[0]
            self.assertEqual(file["file_name"], "target-wt-snp-indel.mafplus.txt")
            self.assertEqual(file.acl, [])
            self.assertEqual(file["md5sum"], '5a7146f821d11c8fa91a0f5865f7b6f8')
            self.assertEqual(subtype["name"], "Simple somatic mutation")
