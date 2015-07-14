from base import ZugTestBase
from zug.datamodel.tcga_connect_bio_xml_nodes_to_case\
    import TCGABioXMLCaseConnector as Connector
from gdcdatamodel.models import Case, File, FileDescribesCase


class TestTCGABioXMLCaseConnector(ZugTestBase):

    def insert_required_nodes(self):
        self.barcode = 'test-barcode'
        self.bio_name = Connector.biospecimen_names[0].format(
            barcode=self.barcode)
        self.clin_name = Connector.clinical_names[0].format(
            barcode=self.barcode)
        self.case = self.get_fuzzed_node(
            Case, submitter_id=self.barcode)
        self.biospec = self.get_fuzzed_node(
            File, file_name=self.bio_name, state='live')
        self.clinical = self.get_fuzzed_node(
            File, file_name=self.clin_name, state='submitted')
        self.biospec.sysan['source'] = 'tcga_dcc'
        self.clinical.sysan['source'] = 'tcga_dcc'
        with self.graph.session_scope() as s:
            s.merge(self.case)
            s.merge(self.biospec)
            s.merge(self.clinical)

    def test_simple_edge_connection(self):
        self.insert_required_nodes()
        conn = Connector(self.graph)
        conn.run()
        with self.graph.session_scope():
            self.assertEqual(
                self.graph.edges(FileDescribesCase).count(), 2)

    def test_single_edge_connection(self):
        self.insert_required_nodes()
        conn = Connector(self.graph)
        conn.connect_files_to_case([self.biospec, self.clinical])
        with self.graph.session_scope():
            self.assertEqual(
                self.graph.edges(FileDescribesCase).count(), 2)