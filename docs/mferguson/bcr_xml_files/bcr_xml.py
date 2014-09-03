#!/usr/bin/python3
'''
Created on Apr 13, 2014

@author: martin
'''

# XML library
#from lxml import etree  # @UnresolvedImport
import datetime
    
class BcrXmlClass(object):
    '''
    Base class for any BCR XML instance type: clinical, biospecimen, auxiliary, cqcf, follow_up, etc.
    '''

    def __init__(self, params):
        '''
        Constructor: take filehandle, XML text, or XML file name, or dictionary of all params. Read, parse, and set instance variables
        '''
        
        self.object_dictionary = params.copy()
        
        # Remove the xml entry - keep separate
        self.object_dictionary.pop('xml')
        # Keep this separate
        self.xml               = params['xml']

    def get_all(self):
        '''
        Returns a dictionary of all the main data elements, including XML
        '''
        self.object_dictionary['xml'] = self.get_xml()
        return self.object_dictionary
    
    def get_all_but_xml(self):
        '''
        Returns a dictionary of all the main data elements, NOT including XML
        '''
        return self.object_dictionary    

    def get_xml(self):
        #return etree.tostring(self.tree,encoding='unicode')
        return self.xml
        
    def get_xml_type(self):
        return self.object_dictionary['xml_type']
    
    def get_disease_code(self):
        return self.object_dictionary['disease_code']

    def get_tss_id(self):
        return self.object_dictionary['tss_id']
    
    def get_batch(self):
        return self.object_dictionary['batch']

    def get_revision(self):
        return self.object_dictionary['revision']

    def get_case_id(self):
        return self.object_dictionary['case_id']

class BcrClinXmlClass(BcrXmlClass):
    
    def __init__(self, params):
        '''
        Constructor: take dict of all required parameters. Call parent. Parse and set instance variables
        '''
        super().__init__(params)

class BcrBiospXmlClass(BcrXmlClass):
    
    def __init__(self, params):
        '''
        Constructor: take dict of all required parameters. Call parent. Parse and set instance variables
        '''
        super().__init__(params)

# End of BcrClinXmlClass
#########################################################################

        
# Module test
if __name__ == '__main__':
    
    print("Testing BcrXml object classes - not using factory.")

    test_clinical_xml_file_name     = "./nationwidechildrens.org_clinical.TCGA-G2-A2EF.1.xml"    
    test_clinical_xml_file_handle = open(test_clinical_xml_file_name ,'r')
    test_clinical_xml             = test_clinical_xml_file_handle.read()
    test_clinical_xml_file_handle = test_clinical_xml_file_handle.close()
    test_dict = {            
        'dcc_submission_date' : datetime.date(2014, 11, 5),
        'project_code'        : 'TCGA',
        'disease_code'        : 'BLCA',
        'batch'               : 175,
        'revision'            : 42,
        'case_id'             : 'A2EF',
        'case_uuid'           : '2A142731-119C-495D-AD30-EB0B48BACB46',
        'schema_version'      : 2.6,
        'xml_type'            : 'clinical',
        'xml'                 : test_clinical_xml
                    }
    test_BCR_XML_object = BcrClinXmlClass(test_dict)
        
    print (test_BCR_XML_object)
    print (test_BCR_XML_object.get_all_but_xml())
