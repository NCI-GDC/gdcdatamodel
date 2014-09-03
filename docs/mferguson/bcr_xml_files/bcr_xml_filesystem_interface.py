'''
Created on May 1, 2014

@author: martin
'''

import glob
import os


# Constants
DEF_BASE_DIR    = '/home/martin/my_documents/consulting_practice/NCI/TCGA/05_dcc/all'
DEF_FILE_FILTER = '*Level_1*/*clinical*.xml'
#DEF_FILE_FILTER = 'nationwidechildrens.org_OV.bio.Level_1.40.31.0/*clinical*.xml'
#files = glob.glob("nationwidechildrens.org_OV.bio.Level_1.*.*.0/*clinical*.xml")
#files = glob.glob("./nationwidechildrens.org_clinical.TCGA-G2-A2EF.*.xml")

class FilesystemClinXmlGetter(object):
    '''
    Class for interacting specifically with the filesystem organization inherent with BCR XML files
    '''


    def __init__(self, base_dir = DEF_BASE_DIR):
        '''
        Constructor
        '''
        
        # Change working dir to base_dir, arg or default
        os.chdir(base_dir)        
        
    
    def get_list_of_xml_files(self, fname_filter = DEF_FILE_FILTER):

        #os.chdir(DEF_BASE_DIR)        
        files = glob.glob(fname_filter)
    
        return files
    
    def get_filesystem_xml(self, dir_fname_location):
        file_handle = open(dir_fname_location ,'r')
        xml         = file_handle.read()
        file_handle.close()
        
        return xml

