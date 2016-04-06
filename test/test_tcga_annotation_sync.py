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
        sysans = {
            'group_id': 'OV_13',
            'version': 81
        }
        aliquot.system_annotations = sysans

        with self.graph.session_scope() as session:
            session.merge(aliquot)
        return aliquot

    def create_case(self, barcode):
        case = Case(
            node_id=str(uuid4()),
            submitter_id=barcode
        )
        sysans = {
            'group_id': 'THCA_176',
            'version': 83
        }
        case.system_annotations = sysans
        with self.graph.session_scope() as session:
            session.merge(case)
        return case

    def create_portion(self, barcode):
        portion = Portion(
            node_id=str(uuid4()),
            submitter_id=barcode,
            portion_number="30",
            creation_datetime=123456,
        )
        sysans = {
            'group_id': 'KIRC_65',
            'version': 88
        }
        portion.system_annotations = sysans
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
        case = self.create_case("TCGA-BG-A0MS")
        portion = self.create_portion("TCGA-XV-AB01-06A-21-A444-20")
        with HTTMock(mock_annotations(FAKE_ANNOTATIONS_WITH_MUNGE)):
            syncer = TCGAAnnotationSyncer()
            syncer.go()
        with self.graph.session_scope():
            self.graph.nodes(Annotation)\
                      .filter(Annotation.cases.contains(case))\
                      .one()
            self.graph.nodes(Annotation)\
                      .filter(Annotation.portions.contains(portion))\
                      .one()

    def test_annotation_acl(self):
        self.create_aliquot("TCGA-06-0237-01A-02D-0234-02")
        self.create_aliquot("TCGA-06-0237-10A-01D-0235-02")
        with HTTMock(mock_annotations(FAKE_ANNOTATIONS)):
            syncer = TCGAAnnotationSyncer()
            syncer.go()
        with self.graph.session_scope():
            annotations = self.graph.nodes(Annotation).all()
        self.assertEqual(len(annotations), 2)
        for annotation in annotations:
            self.assertEqual(annotation.acl, ["open"])

    def test_idempotency(self):
        self.create_aliquot("TCGA-06-0237-01A-02D-0234-02")
        self.create_aliquot("TCGA-06-0237-10A-01D-0235-02")

        # First round insert
        with HTTMock(mock_annotations(FAKE_ANNOTATIONS)):
            syncer = TCGAAnnotationSyncer()
            syncer.go()

        # Change a property
        with self.graph.session_scope():
            for annotation in self.graph.nodes(Annotation):
                annotation.acl = []


        # Re-sync and make sure changes are written
        with HTTMock(mock_annotations(FAKE_ANNOTATIONS)):
            syncer = TCGAAnnotationSyncer()
            syncer.go()
        with self.graph.session_scope():
            for annotation in self.graph.nodes(Annotation):
                self.assertEqual(annotation.acl, ["open"])
