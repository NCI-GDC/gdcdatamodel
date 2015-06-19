import os
from uuid import uuid4

from base import ZugTestBase
from gdcdatamodel.models import Annotation, Aliquot, Case, Portion

from zug.datamodel.tcga_annotations import TCGAAnnotationSyncer

from httmock import HTTMock, urlmatch

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
FIXTURES_DIR = os.path.join(TEST_DIR, "fixtures")

FAKE_ANNOTATIONS = open(os.path.join(FIXTURES_DIR, "fake_tcga_annotations.json")).read()
FAKE_ANNOTATIONS_WITH_MUNGE = open(os.path.join(FIXTURES_DIR, "fake_tcga_annotations_with_munge.json")).read()


def mock_annotations(doc):
    @urlmatch(netloc=r'.*tcga-data.*')
    def inner(url, request):
        return {"content": doc,
                "status_code": 200}
    return inner


class TCGAAnnotationTest(ZugTestBase):

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
        return aliquot

    def create_case(self, barcode):
        part = Case(
            node_id=str(uuid4()),
            submitter_id=barcode
        )
        with self.graph.session_scope() as session:
            session.merge(part)
        return part

    def create_portion(self, barcode):
        portion = Portion(
            node_id=str(uuid4()),
            submitter_id=barcode,
            portion_number="30",
            creation_datetime=123456,
        )
        with self.graph.session_scope() as session:
            session.merge(portion)
        return portion

    def test_basic_sync_works(self):
        self.create_aliquot("TCGA-06-0237-01A-02D-0234-02")
        self.create_aliquot("TCGA-06-0237-10A-01D-0235-02")
        for _ in range(1):
            with HTTMock(mock_annotations(FAKE_ANNOTATIONS)):
                syncer = TCGAAnnotationSyncer()
                syncer.go()
            with self.graph.session_scope():
                self.assertEqual(self.graph.nodes(Annotation).count(), 2)
                self.assertIsNotNone(self.graph.nodes(Aliquot)
                                     .props(submitter_id="TCGA-06-0237-01A-02D-0234-02")
                                     .one().annotations)

    def test_sync_with_name_munging(self):
        part = self.create_case("TCGA-BG-A0MS")
        portion = self.create_portion("TCGA-XV-AB01-06A-21-A444-20")
        with HTTMock(mock_annotations(FAKE_ANNOTATIONS_WITH_MUNGE)):
            syncer = TCGAAnnotationSyncer()
            syncer.go()
        with self.graph.session_scope():
            self.graph.nodes(Annotation)\
                      .filter(Annotation.cases.contains(part))\
                      .one()
            self.graph.nodes(Annotation)\
                      .filter(Annotation.portions.contains(portion))\
                      .one()
