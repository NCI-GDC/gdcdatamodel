from base import ZugsTestBase
from zug.datamodel.tcga_connect_bio_xml_nodes_to_participant\
    import TCGABioXMLParticipantConnector as Connector
from gdcdatamodel.models import Participant, File, FileDescribesParticipant


class TestTCGABioXMLParticipantConnector(ZugsTestBase):

    def insert_required_nodes(self):
        self.barcode = 'test-barcode'
        self.bio_name = Connector.biospecimen_names[0].format(
            barcode=self.barcode)
        self.clin_name = Connector.clinical_names[0].format(
            barcode=self.barcode)
        self.participant = self.get_fuzzed_node(
            Participant, submitter_id=self.barcode)
        self.biospec = self.get_fuzzed_node(
            File, file_name=self.bio_name, state='live')
        self.clinical = self.get_fuzzed_node(
            File, file_name=self.clin_name, state='submitted')
        self.biospec.sysan['source'] = 'tcga_dcc'
        self.clinical.sysan['source'] = 'tcga_dcc'
        with self.graph.session_scope() as s:
            s.merge(self.participant)
            s.merge(self.biospec)
            s.merge(self.clinical)

    def test_simple_edge_connection(self):
        self.insert_required_nodes()
        conn = Connector(self.graph)
        conn.run()
        with self.graph.session_scope():
            self.assertEqual(
                self.graph.edges(FileDescribesParticipant).count(), 2)

    def test_single_edge_connection(self):
        self.insert_required_nodes()
        conn = Connector(self.graph)
        conn.connect_files_to_participant([self.biospec, self.clinical])
        with self.graph.session_scope():
            self.assertEqual(
                self.graph.edges(FileDescribesParticipant).count(), 2)
