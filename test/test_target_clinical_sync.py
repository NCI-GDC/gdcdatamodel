import os
import uuid

from base import ZugTestBase, TEST_DIR

from httmock import HTTMock, urlmatch

from zug.datamodel.target.sample_matrices import NAMESPACE_CASES
from zug.datamodel.target.clinical import TARGETClinicalSyncer
from gdcdatamodel.models import (
    File,
    Demographic,
    Diagnosis,
    FileDescribesCase
)

FIXTURES_DIR = os.path.join(TEST_DIR, "fixtures")


@urlmatch(netloc='target-data.nci.nih.gov')
def target_clinical_mock(url, request):
    filename = url.path.split('/')[-1]
    content = open(os.path.join(FIXTURES_DIR, filename)).read()
    return {'content': content,
            'headers': {'Content-Length': str(len(content))}}


class TARGETClinicalSyncerTest(ZugTestBase):

    def create_file(self, url):
        return self.graph.node_merge(
            node_id=str(uuid.uuid4()),
            label="file",
            properties={
                "file_name": url.split("/")[-1],
                "md5sum": "bogus",
                "file_size": 0,
                "state": "live"
            },
            system_annotations={
                "source": "target_dcc",
                "url": url,
            }
        )

    def create_case(self, barcode):
        return self.graph.node_merge(
            node_id=str(uuid.uuid5(NAMESPACE_CASES, barcode)),
            label="case",
            properties={
                "submitter_id": barcode,
            },
            system_annotations={
                "source": "target_sample_matrcies"
            }
        )

    def test_basic_sync(self):
        ROOT_TEST_URL = "https://target-data.nci.nih.gov/Public/WT/clinical/"
        self.create_file("%stest_target_clinical_19911205.xlsx" % ROOT_TEST_URL)
        case = self.create_case("TARGET-50-ABCDEF")
        syncer = TARGETClinicalSyncer(
            "WT", 
            "%stest_target_clinical_19911205.xlsx" % ROOT_TEST_URL,
            graph=self.graph
        )
        with HTTMock(target_clinical_mock):
            syncer.sync()
        with self.graph.session_scope():
            diagnosis = self.graph.nodes(Diagnosis).filter(Diagnosis.cases.contains(case)).one()
            demographic = self.graph.nodes(Demographic).filter(Demographic.cases.contains(case)).one()
            self.assertEqual(diagnosis["vital_status"], "dead")
            self.assertEqual(demographic["gender"], "male")
            self.assertEqual(demographic["race"], "white")
            self.assertEqual(demographic["ethnicity"], "not hispanic or latino")
            self.assertEqual(diagnosis["age_at_diagnosis"], 123)
            self.assertEqual(diagnosis['morphology'], 'not reported')
            # make sure the file now describes the case
            self.graph.nodes(File)\
                      .sysan({"url": "%stest_target_clinical_19911205.xlsx" % ROOT_TEST_URL})\
                      .with_edge_to_node(FileDescribesCase, case).one()

    def test_sync_optional_fields(self):
        ROOT_TEST_URL = "https://target-data.nci.nih.gov/Public/WT/clinical/"
        self.create_file("%stest_target_clinical_19911206.xlsx" % ROOT_TEST_URL)
        case = self.create_case("TARGET-50-ABCDEF")
        syncer = TARGETClinicalSyncer(
            "WT", 
            "%stest_target_clinical_19911206.xlsx" % ROOT_TEST_URL,
            graph=self.graph
        )
        with HTTMock(target_clinical_mock):
            syncer.sync()
        with self.graph.session_scope():
            diagnosis = self.graph.nodes(Diagnosis).filter(Diagnosis.cases.contains(case)).one()
            demographic = self.graph.nodes(Demographic).filter(Demographic.cases.contains(case)).one()
            self.assertEqual(diagnosis["vital_status"], "dead")
            self.assertEqual(demographic["gender"], "male")
            self.assertEqual(demographic["race"], "white")
            self.assertEqual(demographic["ethnicity"], "not hispanic or latino")
            self.assertEqual(diagnosis["age_at_diagnosis"], 123)
            self.assertEqual(diagnosis['tumor_stage'], 'iiib')
            self.assertEqual(diagnosis['morphology'], '8963/3')
            self.assertEqual(diagnosis['site_of_resection_or_biopsy'], 'c000')
            # make sure the file now describes the case
            self.graph.nodes(File)\
                      .sysan({"url": "%stest_target_clinical_19911206.xlsx" % ROOT_TEST_URL})\
                      .with_edge_to_node(FileDescribesCase, case).one()

