from base import ZugsTestBase, TEST_DIR
from mock import patch
from httmock import HTTMock, urlmatch

import os
from gdcdatamodel.models import (
    File,
    FileDataFromAliquot,
    FileRelatedToFile,
)
from zug.datamodel.target.magetab_sync import TARGETMAGETABSyncer

FIXTURES_DIR = os.path.join(TEST_DIR, "fixtures", "target_magetabs")


@urlmatch(netloc='target-data.nci.nih.gov')
def sdrf_mock(url, request):
    content = open(os.path.join(FIXTURES_DIR, "test_magetab_19911205.sdrf.txt")).read()
    return content


def fake_tree_walk(url, **kwargs):
    for url in ["https://target-data.nci.nih.gov/WT/Discovery/WXS/L3/mutation/BCM/test_magetab_19911205.sdrf.txt"]:
        yield url


class TARGETMAGETABSyncTest(ZugsTestBase):

    def create_aliquot(self, barcode):
        return self.graph.node_merge(
            node_id=barcode,
            label="aliquot",
            properties={
                "submitter_id": barcode,
            },
            system_annotations={
                "source": "target_sample_matrices"
            }
        )

    def create_file(self, name):
        return self.graph.node_merge(
            node_id=name,
            label="file",
            properties={
                "file_name": name,
                "md5sum": "bogus",
                "file_size": 1,
                "state": "live"
            },
            system_annotations={
                "source": "target_dcc"
            }
        )

    @patch("zug.datamodel.target.magetab_sync.tree_walk", fake_tree_walk)
    def test_basic_magetab_sync(self):
        sdrf = self.create_file("test_magetab_19911205.sdrf.txt")
        aliquot = self.create_aliquot("TARGET-50-CAAAAB-01A-01D")
        file_name = "A04159_10_lanes_dupsFlagged.varFilter.anno1.maf"
        self.create_file(file_name)
        syncer = TARGETMAGETABSyncer("WT", graph=self.graph)
        with HTTMock(sdrf_mock):
            syncer.sync()
        with self.graph.session_scope():
            self.graph.nodes(File)\
                      .with_edge_to_node(FileDataFromAliquot, aliquot)\
                      .with_edge_from_node(FileRelatedToFile, sdrf)\
                      .props(file_name=file_name)\
                      .one()
            edges = self.graph.edges().sysan({
                "source": "target_magetab",
                "sdrf_name": "test_magetab",
                "sdrf_version": 727171
            }).all()
            self.assertEqual(len(edges), 2)
