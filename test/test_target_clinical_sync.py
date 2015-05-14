import os
import uuid

from base import ZugsTestBase, TEST_DIR

from httmock import HTTMock, urlmatch

from zug.datamodel.target.sample_matrices import NAMESPACE_PARTICIPANTS
from zug.datamodel.target.clinical import TARGETClinicalSyncer
from gdcdatamodel.models import (
    File,
    ClinicalDescribesParticipant,
    FileDescribesParticipant
)

FIXTURES_DIR = os.path.join(TEST_DIR, "fixtures")


@urlmatch(netloc='target-data.nci.nih.gov')
def target_clinical_mock(url, request):
    content = open(os.path.join(FIXTURES_DIR, "test_target_clinical_19911205.xlsx")).read()
    return {'content': content,
            'headers': {'Content-Length': str(len(content))}}


class TARGETClinicalSyncerTest(ZugsTestBase):

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

    def create_participant(self, barcode):
        return self.graph.node_merge(
            node_id=str(uuid.uuid5(NAMESPACE_PARTICIPANTS, barcode)),
            label="participant",
            properties={
                "submitter_id": barcode,
            },
            system_annotations={
                "source": "target_sample_matrcies"
            }
        )

    def test_basic_sync(self):
        self.create_file("https://target-data.nci.nih.gov/WT/Discovery/clinical/test_target_clinical_19911205.xlsx")
        participant = self.create_participant("TARGET-50-ABCDEF")
        syncer = TARGETClinicalSyncer("WT", "https://target-data.nci.nih.gov/WT/Discovery/clinical/test_target_clinical_19911205.xlsx",
                                      graph=self.graph)
        with HTTMock(target_clinical_mock):
            syncer.sync()
        with self.graph.session_scope():
            clin = self.graph.nodes(File).with_edge_to_node(ClinicalDescribesParticipant, participant).one()
            self.assertEqual(clin["vital_status"], "dead")
            self.assertEqual(clin["gender"], "male")
            self.assertEqual(clin["race"], "white")
            self.assertEqual(clin["ethnicity"], "not hispanic or latino")
            self.assertEqual(clin["age_at_diagnosis"], 123)
            # make sure the file now describes the participant
            self.graph.nodes(File)\
                      .sysan({"url": "https://target-data.nci.nih.gov/WT/Discovery/clinical/test_target_clinical_19911205.xlsx"})\
                      .with_edge_to_node(FileDescribesParticipant, participant).one()
