import os
from zug.datamodel import xml2psqlgraph, clinical_xml_mapping
from base import ZugTestBase, TEST_DIR, PreludeMixin
from gdcdatamodel.models import Case, Diagnosis, Treatment


class TestTCGAClinicalImport(ZugTestBase):


    def setUp(self):
        super(TestTCGAClinicalImport, self).setUp()
        with self.g.session_scope() as s:
            # case id from clinical xml bcr_patient_uuid
            case = Case("368e23f0-e573-4547-bf5a-14080baf737b")
            s.merge(case)
            self.case_id = case.node_id
        self.converter = xml2psqlgraph.xml2psqlgraph(
            xml_mapping=clinical_xml_mapping,
            **self.graph_info)

    def test_import_clinical(self):
        with open(os.path.join(TEST_DIR, 'tcga_clinical.xml')) as f:
            xml = f.read()
        self.converter.xml2psqlgraph(xml)
        self.converter.export(group_id='group1', version=1)
        with self.g.session_scope() as s:
            diagnosis = self.g.nodes(Diagnosis).one()
            # check vital status is from latest followup
            assert diagnosis.vital_status == "dead"

            # check default attributes
            assert diagnosis.classification_of_tumor == 'not reported'

            assert diagnosis.submitter_id == "TCGA-50-5930_diagnosis"
            treatment = self.g.nodes(Treatment).one()
            assert treatment.diagnoses[0] == diagnosis

            assert diagnosis.cases[0].node_id == self.case_id