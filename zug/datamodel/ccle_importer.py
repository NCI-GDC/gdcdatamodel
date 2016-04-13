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
import xlrd
import psqlgraph
from . import PKG_DIR
from sqlalchemy.orm.exc import NoResultFound
from cdisutils.log import get_logger

class CCLEImporter(object):

    def __init__(self, host, user, password, db,
                  excel_path='ccle_data.xlsx',
                  disease_path='diseaseStudy.txt'):

        self.log = get_logger("ccle_import")
        self.g = psqlgraph.PsqlGraphDriver(
                  host=host, user=user,
                  password=password,
                  database=db)
        self.disease_map = self.load_diseases(disease_path)
        self.program_node = None
        self.project_node_map = {}
        self.data  = xlrd.open_workbook(excel_path).sheet_by_index(0)


    def make_program_project(self):
        '''
        Create ccle program and project nodes if they don't exist
        Separated from __init__ so we can run within a roll-backable scope
        '''
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

    def import_from_excel(self):
        for rx in range(20, self.data.nrows):
            self.make_nodes(self.data.row(rx))

    def make_nodes(self, rx):
        '''
        Processes a row from the spreadsheet
        '''
        aliquot_node = self.make_aliquot(rx)
        sample_node = self.make_sample(rx)
        if sample_node not in aliquot_node.samples:
            aliquot_node.samples.append(sample_node)
        case_node = self.make_case(rx)
        if case_node not in sample_node.cases:
            sample_node.cases.append(case_node)

    def make_aliquot(self, rx):
        '''
        Make a new aliquot, or return existing
        '''
        submitter_id = rx[15].value
        n = self.g.nodes(Aliquot).props({'submitter_id':submitter_id}).first()
        if n:
            self.log.info('Aliquot already exists: {}'.format(submitter_id))
            return n
        else:
            center = rx[19].value
            center_code = rx[18].value
            try:
                center = self.g.nodes(Center).props({'code':center_code}).one()
            except NoResultFound:
                self.log.warn('Center with id {} not found'.format(center_code))
                return None
            
            cancer_code = rx[8].value
            project_id = 'CCLE-{}'.format(cancer_code)
            analyte_type = rx[13].value
            analyte_type_id = rx[12].value
            n = Aliquot(node_id=str(uuid4()),
                        submitter_id=submitter_id,
                        analyte_type=analyte_type,
                        analyte_type_id=analyte_type_id,
                        project_id=project_id)
        
            n.centers.append(center)
            self.g.current_session().add(n)
            self.log.info('Made new aliquot: {}'.format(submitter_id))
        return n

    def make_sample(self, rx):
        '''
        Makes a new sample node
        '''
        submitter_id = rx[9].value
        n = self.g.nodes(Sample).props({'submitter_id':submitter_id}).first()
        if n:
            self.log.info('Sample already exists: {}'.format(submitter_id))
            return n
        else:
            sample_type = rx[11].value
            if sample_type.lower().strip() == 'cell line':
                sample_type = 'Cell Lines'
            elif sample_type.lower().strip() == 'ebv immortalized':
                sample_type = 'EBV Immortalized Normal'

            sample_type_code = rx[10].value

            cancer_code = rx[8].value

            project_id = 'CCLE-{}'.format(cancer_code)
            
            if str(sample_type_code) == '':
                n = Sample(node_id=str(uuid4()),
                          submitter_id=submitter_id)
            else:
                n = Sample(node_id=str(uuid4()),
                          submitter_id=submitter_id,
                          sample_type=sample_type,
                          sample_type_id=str(int(sample_type_code)))
                self.g.current_session().add(n)
            self.log.info('Made new sample: {}'.format(submitter_id))

        return n

    def make_case(self, rx):
        '''
        Make a new case, or return existing
        '''
        submitter_id = rx[2].value

        n = self.g.nodes(Case).props({'submitter_id':submitter_id}).first()
        if n:
            self.log.info('Case already exists: {}'.format(submitter_id))
            return n
        else:
            cancer_code = rx[8].value
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
            self.g.current_session().add(n)
            self.log.info('Made new case: {}'.format(submitter_id))

        return n

    def make_program(self):
        '''
        Makes the ccle program node if it doesn't exist,
        otherwise, gets and return the node if it does
        '''
        n = self.g.nodes(Program)\
                .filter(Program._props['name']\
                    .astext=='CCLE')\
                .first()
        if n:
            self.log.info('Found CCLE Program')
            return n
        else:
            n = Program(node_id=str(uuid4()),
                        dbgap_accession_number='CCLE',
                        name='CCLE')
            self.g.current_session().add(n)
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
            self.g.current_session().add(n)
            self.log.info('Created new {} Project'.format(dname))
        return n

