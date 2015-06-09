from cdisutils.log import get_logger
from gdcdatamodel.models import File, Participant, FileDescribesParticipant
import random
import re


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
        barcode_regex = '([a-zA-Z0-9-]+)'
        self.bio_regexes = [re.compile(n.format(barcode=barcode_regex))
                            for n in self.biospecimen_names]
        self.clinical_regexes = [re.compile(n.format(barcode=barcode_regex))
                                 for n in self.clinical_names]
        self.log = get_logger(
            "tcga_connect_bio_xml_nodes_to_participant_{}".format(
                random.randint(1e5, 1e6)))

    def run(self, dry_run=False):
        assert self.g, 'No driver provided'
        if dry_run:
            self.log.info('User requested dry run.')
        with self.g.session_scope() as session:
            self.connect_all()
            if dry_run:
                self.log.info('Rolling back dry run session.')
                session.rollback()

    def connect_all(self):
        """Loops through all participants and creates an edge to any xml files
        whose name matches the above schemes.

        """

        self.log.info('Loading participants')
        participants = self.g.nodes(Participant).all()
        self.log.info('Found {} participants'.format(len(participants)))

        self.log.info('Loading xml files')
        xmls = {
            n['file_name']: n for n in self.g.nodes(File)
            .sysan({'source': 'tcga_dcc'})
            .filter(File.file_name.astext.endswith('.xml'))
            .all()
        }
        self.log.info('Found {} xml files'.format(len(xmls)))

        for participant in participants:
            self.connect_participant(xmls, participant)

    def file_matches_regexes(self, file_name):
        """Returns the barcode if matches class regexes.  If it doesn't match,
        return ``None``.

        """
        print file_name
        for regex in self.bio_regexes+self.clinical_regexes:
            match = regex.match(file_name)
            if match:
                return match.group(1)

    def connect_files_to_participant(self, file_nodes):
        """Takes a list of file nodes and, if applicable, connects them to
        participants. If not applicale, no action is taken.

        :param file_nodes: iterable containing file node instances

        """

        with self.g.session_scope():
            self.log.info('Attempting to connect {} to participants'.format(
                file_nodes))

            # Filter for applicability
            xmls = {n.file_name: n for n in file_nodes
                    if self.file_matches_regexes(n.file_name)}
            self.log.info('Found matching {} files: {}'.format(
                len(xmls), xmls))

            # Get barcodes
            barcodes = {self.file_matches_regexes(file_name)
                        for file_name in xmls.keys()}
            self.log.info('Found matching {} files with barcodes: {}'.format(
                len(barcodes), barcodes))

            # Get participants
            participants = {
                p for p in self.g.nodes(Participant)
                .prop_in('submitter_id', list(barcodes)).all()
            }
            self.log.info('Found matching {} participants: {}'.format(
                len(participants), participants))

            # Create edges
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

        self.log.info('Looking for xml files for {}'.format(participant))
        barcode = participant['submitter_id']
        p_neighbor_ids = [e.src_id for e in participant.edges_in]

        # Lookup clinical node and insert an edge if found
        clinical_nodes = [xmls.get(n.format(barcode=barcode))
                          for n in self.clinical_names
                          if xmls.get(n.format(barcode=barcode))]
        for clinical in clinical_nodes:
            if clinical.node_id not in p_neighbor_ids:
                self.log.info('Adding edge to clinical xml {} for {}'.format(
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
                self.log.info('Adding edge to biospecimen xml {} for {}'.format(
                    biospecimen, participant))
                self.g.current_session().merge(FileDescribesParticipant(
                    src_id=biospecimen.node_id,
                    dst_id=participant.node_id
                ))

        # If we didn't find any biospecimen nodes, log a warning
        if not biospecimen_nodes:
            self.log.warn('Missing biospecimen file for {} with barcode {}'.format(
                participant, barcode))
