import os
from uuid import uuid4

from base import ZugsTestBase
from gdcdatamodel.models import Annotation, Aliquot

from zug.datamodel.tcga_annotations import TCGAAnnotationSyncer

from httmock import HTTMock, urlmatch

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
FIXTURES_DIR = os.path.join(TEST_DIR, "fixtures")

FAKE_ANNOTATIONS = open(os.path.join(FIXTURES_DIR, "fake_tcga_annotations.json")).read()


@urlmatch(netloc=r'.*tcga-data.*')
def mock_tcga_annotations(url, request):
    return {"content": FAKE_ANNOTATIONS,
            "status_code": 200}


class TCGAAnnotationTest(ZugsTestBase):

    def create_aliquot(self, barcode):
        aliquot = Aliquot(
            node_id=str(uuid4()),
            submitter_id=barcode,
            source_center="foo",
            amount=3.5,
            concentration=10.0
        )
        with self.graph.session_scope() as session:
            session.merge(aliquot)

    def setUp(self):
        super(TCGAAnnotationTest, self).setUp()
        os.environ["PG_HOST"] = "localhost"
        os.environ["PG_USER"] = "test"
        os.environ["PG_PASS"] = "test"
        os.environ["PG_NAME"] = "automated_test"

    def test_basic_sync_works(self):
        self.create_aliquot("TCGA-06-0237-01A-02D-0234-02")
        self.create_aliquot("TCGA-06-0237-10A-01D-0235-02")
        with HTTMock(mock_tcga_annotations):
            syncer = TCGAAnnotationSyncer()
            syncer.go()
        with self.graph.session_scope():
            self.assertEqual(self.graph.nodes(Annotation).count(), 2)
