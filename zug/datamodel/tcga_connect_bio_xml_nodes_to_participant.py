from cdisutils.log import get_logger
from gdcdatamodel.models import File, Participant, FileDescribesParticipant

log = get_logger("tcga_connect_bio_xml_nodes_to_participant")


class TCGABioXMLParticipantConnector(object):

    biospecimen_names = [
        'nationwidechildrens.org_biospecimen.{barcode}.xml',
        'nationwidechildrens.org_control.{barcode}.xml',
        'genome.wustl.edu_biospecimen.{barcode}.xml',
        'genome.wustl.edu_control.{barcode}.xml',
    ]

    clinical_names = [
        'nationwidechildrens.org_clinical.{barcode}.xml',
        'genome.wustl.edu_clinical.{barcode}.xml',
    ]

    def __init__(self, graph):
        self.g = graph

    def run(self):
        assert self.g, 'No driver provided'
        with self.g.session_scope():
            self.connect_all()

    def connect_all(self):
        """Loops through all participants and creates an edge to any xml files
        whose name matches the above schemes.

        """

        log.info('Loading participants')
        participants = self.g.nodes(Participant).all()
        log.info('Found {} participants'.format(len(participants)))

        log.info('Loading xml files')
        xmls = {
            n['file_name']: n for n in self.g.nodes(File)
            .sysan({'source': 'tcga_dcc'})
            .filter(File.file_name.astext.endswith('.xml'))
            .all()
        }
        log.info('Found {} xml files'.format(len(xmls)))

        for participant in participants:
            self.connect_participant(xmls, participant)

    def connect_participant(self, xmls, participant):
        """Takes a dictionary of xml file nodes (keyed by name) and a
        participant node and creates an edge between any xml files tha
        correspond to the participant.

        :param g: PsqlGraphDriver
        :param dict xmls: All xml nodes in the database keyed by file_name
        :param Participant participant: participant to connect to xml nodes

        """

        log.info('Looking for xml files for {}'.format(participant))
        barcode = participant['submitter_id']
        p_neighbor_ids = [e.src_id for e in participant.edges_in]

        # Lookup clinical node and insert an edge if found
        clinical_nodes = [xmls.get(n.format(barcode=barcode))
                          for n in self.clinical_names
                          if xmls.get(n.format(barcode=barcode))]
        for clinical in clinical_nodes:
            if clinical.node_id not in p_neighbor_ids:
                log.info('Adding edge to clinical xml {} for {}'.format(
                    clinical, participant))
                self.g.current_session().merge(FileDescribesParticipant(
                    src_id=clinical.node_id,
                    dst_id=participant.node_id,
                ))

        # Lookup biospecimen node and insert an edge if found
        biospecimen_nodes = [xmls.get(n.format(barcode=barcode))
                             for n in self.biospecimen_names
                             if xmls.get(n.format(barcode=barcode))]
        for biospecimen in biospecimen_nodes:
            if biospecimen.node_id not in p_neighbor_ids:
                log.info('Adding edge to biospecimen xml {} for {}'.format(
                    biospecimen, participant))
                self.g.current_session().merge(FileDescribesParticipant(
                    src_id=biospecimen.node_id,
                    dst_id=participant.node_id
                ))

        # If we didn't find any biospecimen nodes, log a warning
        if not biospecimen_nodes:
            log.warn('Missing biospecimen file for {} with barcode {}'.format(
                participant, barcode))
