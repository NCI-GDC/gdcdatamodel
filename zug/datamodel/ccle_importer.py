import os
from uuid import uuid4
from gdcdatamodel.models import (
        Aliquot,
        Analyte,
        Case,
        Center,
        File,
        Program,
        Project,
        Sample,
)
import psqlgraph
from . import PKG_DIR
from sqlalchemy.orm.exc import NoResultFound
from cdisutils.log import get_logger

class CCLEImporter(object):

    def __init__(self, host, user, password, db, disease_path='diseaseStudy.txt'):

        self.log = get_logger("ccle_import")
        self.g = psqlgraph.PsqlGraphDriver(
                  host=host, user=user,
                  password=password,
                  database=db)
        self.disease_map = self.load_diseases(disease_path)
        self.program_node = None
        self.project_node_map = {}

        with self.g.session_scope() as session:
            # Create ccle program and project nodes if they don't exist
            self.program_node = self.make_program()
            self.log.info('Creating projects')
            self.project_node_map = self.make_projects()

    def load_diseases(self, path):
        '''
        Creates id -> name mappings for diseases from:
        https://tcga-data.nci.nih.gov/datareports/codeTablesReport.htm?codeTable=disease%20study
        '''
        d_map = {}
        with open(os.path.join(PKG_DIR,path)) as f:
            # Read in header
            f.readline()
            for line in f:
                l = line.strip().split('\t')
                d_map[l[0]] = l[1]

        return d_map

    def import_xml_node(self, root):
        '''
        Processes a <Result> node from cghub
        '''
        aliquot_node = self.make_aliquot(root)
        sample_node = self.make_sample(root)
        if sample_node not in aliquot_node.samples:
            aliquot_node.samples.append(sample_node)
        case_node = self.make_case(root)
        if case_node not in sample_node.cases:
            sample_node.cases.append(case_node)
        # Try to find corresponding file nodes
        self.link_files(root, aliquot_node)

    def link_files(self, root, aliquot):
        '''
        Tries to find file nodes for the aliquot in the graph and link them
        '''
        files = root.xpath('./files/file')
        for file in files:
            props = {}
            for f in file:
                props[f.tag] = f.text
            try:
                f = self.g.nodes(File).props(props).one()
            except NoResultFound:
                self.log.info('No file node found: {}'.format(props['filename']))
            else:
                aliquot.files.append(f)

    def make_aliquot(self, root):
        '''
        Make a new aliquot, or return existing
        '''
        submitter_id = root.xpath('./aliquot_id')[0].text
        n = self.g.nodes(Aliquot).props({'submitter_id':submitter_id}).first()
        if n:
            self.log.info('Aliquot already exists: {}'.format(submitter_id))
            return n
        else:
            center = root.xpath('./center_name')[0]
            # TODO Map these
            if center.text == 'BI':
                center_code = '08'
            else:
                center_code = 'NULL'
            try:
                center = self.g.nodes(Center).props({'code':center_code}).one()
            except NoResultFound:
                self.log.warn('Center with id {} not found'.format(center_code))
                return None
            
            cancer_code = root.xpath('./disease_abbr')[0].text
            project_id = 'CCLE-{}'.format(cancer_code)
            analyte_type_id = root.xpath('./analyte_code')[0].text
            # TODO: Add analyte properties
            n = Aliquot(node_id=str(uuid4()),
                        submitter_id=submitter_id,
                        project_id=project_id)
        
            n.centers.append(center)

            self.log.info('Made new aliquot: {}'.format(submitter_id))
        return n

    def make_sample(self, root):
        '''
        Makes a new sample node
        '''
        submitter_id = root.xpath('./legacy_sample_id')[0].text
        n = self.g.nodes(Sample).props({'submitter_id':submitter_id}).first()
        if n:
            self.log.info('Sample already exists: {}'.format(submitter_id))
            return n
        else:
            sample_type_code = root.xpath('./sample_type')[0].text
            # TODO Map the type code to type
            cancer_code = root.xpath('./disease_abbr')[0].text

            project_id = 'CCLE-{}'.format(cancer_code)
            
            n = Sample(node_id=str(uuid4()),
                      submitter_id=submitter_id,
                      #sample_type=sample_type,
                      sample_type_id=str(sample_type_code))

            self.log.info('Made new sample: {}'.format(submitter_id))

        return n

    def make_case(self, root):
        '''
        Make a new case, or return existing
        '''
        submitter_id = root.xpath('./legacy_sample_id')[0].text
        # id for sample is same as for case
        # but with CCLE prefixed and DNA-08 suffixed
        submitter_id = '-'.join(submitter_id.split('-')[1:-2])

        n = self.g.nodes(Case).props({'submitter_id':submitter_id}).first()
        if n:
            self.log.info('Aliquot already exists: {}'.format(submitter_id))
            return n
        else:
            cancer_code = root.xpath('./disease_abbr')[0].text
            try:
                project_node = self.project_node_map[cancer_code]
            except KeyError:
                self.log.warn('Invalid disease code: {}'.format(cancer_code))
                raise

            project_id = 'CCLE-{}'.format(cancer_code)

            n = Case(node_id=str(uuid4()),
                    submitter_id=submitter_id,
                    project_id=project_id)

            n.projects.append(project_node)

            self.log.info('Made new case: {}'.format(submitter_id))

        return n

    def make_program(self):
        '''
        Makes the ccle program node if it doesn't exist,
        otherwise, gets and return the node if it does
        '''
        n = self.g.nodes(Program)\
                .filter(Program._props['name']\
                    .astext=='Cancer Cell Line Encyclopedia')\
                .first()
        if n:
            self.log.info('Found CCLE Program')
            return n
        else:
            n = Program(node_id=str(uuid4()),
                        dbgap_accession_number='CCLE',
                        name='Cancer Cell Line Encyclopedia')
            self.log.info('Created new CCLE Program')
        return n

    def make_projects(self):
        '''
        Creates projects from the diesease map, if they don't exist
        Returns a new mapping for id -> node relations so we don't have
        to continously query for the appropriate project node
        '''
        node_map = {}
        for (k,v) in self.disease_map.items():
            n = self.make_project(k, v)
            node_map[k] = n

        return node_map

    def make_project(self, dcode, dname):
        '''
        Creates a project node if it does not exist and return it,
        else, return the existing node
        '''
        n = self.g.nodes(Project).props({'name':dname,
                                         'code':dcode,
                                         'dbgap_accession_number':'CCLE'})\
                                 .first()
        if n:
            self.log.info('Found {} Project'.format(dname))
            return n
        else:
            n = Project(node_id=str(uuid4()),
                        name=dname,
                        disease_type=dname,
                        code=dcode,
                        released=True,
                        state='legacy',
                        dbgap_accession_number='CCLE')
            n.programs.append(self.program_node)
            self.log.info('Created new {} Project'.format(dname))
        return n

