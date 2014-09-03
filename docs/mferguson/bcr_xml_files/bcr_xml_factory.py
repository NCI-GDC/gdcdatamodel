#!/usr/bin/python3
'''
Created on Apr 13, 2014

@author: martin

This is a factory class that tries to build a complete data structure (in form of a dict) for entry into the postgres DB,
derived from an XML instance as available at the DCC. There have been such frequent mods of the XSD
over time, however, that for any given instance, the parsing will be very different. This factory has
started with the most recent versions, and started moving backwards in time. Goal would be to eventually
be able to load every instance going back to 2007.

Currently working:
    2.7:    2.7 not out yet
    2.6:    yes
    2.5:    yes
    2.4:    no, but not much work
    <=2.3:  no, much work

'''

# XML library
from lxml import etree  # @UnresolvedImport
#from lxml.etree import fromstring # @UnresolvedImport

from bcr_xml import BcrClinXmlClass, BcrBiospXmlClass
import re
import datetime
import uuid

class BcrXmlClassFactory(object):
    '''
    Factory class that takes arguments and tries to build build an object of the appropriate classe. Throws
    exception if it can't figure out the BCR XML type.
    '''

    def __init__(self, *args, **kwargs):
        pass

    def make_new_bcr_xml_object(self, xml, *args, **kwargs):
        '''
        Return a new bcr xml object, trying several build algorithms
        '''

        try:                      # Build from just XML, or a file of XML, or a FH to xml
            new_bcr_xml_dict = get_bcr_xml_FileOrXmlBased_dict(xml, *args, **kwargs)
        except Exception as e:
            raise (e)
#             try:                # Build from a pre-existing minimal requirements dictionary
#                 pass
#             except:
#                 try:            # Build from DB reference to index
#                     pass
#                 except:
#                     try:        # Build from DB reference to case_id | case_uuid, batch, revision, type
#                         pass
#                     except:
#                         try:    # Build from DB reference to instance_uuid
#                             pass
#                         except:
#                             raise Exception()
        # This section adds a few more parameters, and returns the finished object.
        if (new_bcr_xml_dict['xml_type'] == 'clinical'):
            return BcrClinXmlClass(new_bcr_xml_dict)
        elif (new_bcr_xml_dict['xml_type'] == 'biospecimen'):
            return BcrBiospXmlClass(new_bcr_xml_dict)

    # End new object builder
    ############################################
# End BcrXmlClassFactory class
################################################


def get_bcr_xml_FileOrXmlBased_dict(xml, *args, **kwargsl):
    # Try parsing
    try:    # Try reading as file
        new_tree = etree.parse(xml)
        xml_is_fname = xml
    except OSError:
        # Now try reading as XML string
        from io import BytesIO
        try:    # Try as string
            #xml = str(string(xml, encoding='utf-8'))
            #print("%s" % xml)
            new_tree = etree.parse(BytesIO(xml.encode()))
            #parser = etree.XMLParser(recover=True, encoding='utf-8')
            xml_is_fname = None
        except Exception as e:
            raise (e)
    # Try to get a document root
    try:
        # Try to get a document root
        new_root = new_tree.getroot()
    except Exception as e:
        raise (e)

    # See if we have a schemaVersion in the root
    try:    # Try if schemaVersion = 2.4, 2.5 or 2.6 or higher
        schema_version = float(new_root.attrib.get('schemaVersion'))
    except Exception as e:
        raise (e)

    # Now, decide what to do based on schema_version
    if (schema_version < 2.4):
        raise BadBcrXML ("No parser yet for schemaVersion < 2.4.")
    elif (schema_version == 2.4):
        raise BadBcrXML ("No parser yet for schemaVersion 2.4.")
    elif (schema_version in [2.5, 2.6]):
        bcr_xml_dictionary = parse_25_26_tree(new_tree, xml_is_fname)
    elif (schema_version > 2.6):
        raise BadBcrXML ("No parser yet for schemaVersions > 2.6.")

    # If the input xml is actually a dir/filename, trim any absolute path from front, then store that too
    if (xml_is_fname):
        # Typically: /some/absolute/path/nationwidechildrens.org_OV.bio.Level_1.40.30.0/nationwidechildrens.org_clinical.TCGA-36-2549.xml
        # needs to become: nationwidechildrens.org_OV.bio.Level_1.40.30.0/nationwidechildrens.org_clinical.TCGA-36-2549.xml
        bcr_xml_dictionary['dir_fname_location'] = re.findall(r'.*\/([^\/]*\/[^\/]*)', xml_is_fname)[0]

    return bcr_xml_dictionary
# End work to build object from XML text or file
###################################################

def parse_25_26_tree(tree, fname):
    # Get some attibutes from root node
    root = tree.getroot()
#     print ("Attribs: %s" % root.attrib)
#     print ("NS: %s" % root.nsmap)
    schema_version  = float(root.attrib.get('schemaVersion'))
    # Use schemaLocation to get xml_type
    schema_location = root.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}schemaLocation')
    xml_type = re.findall(r'http://tcga.nci/bcr/xml/(\w+)/',schema_location)[0]

    # Get batch, rev, serial
    [batch, revision, serial] = tree.xpath('//admin:batch_number', namespaces = root.nsmap)[0].text.split('.')
    [batch, revision, serial] = map(int, [batch, revision, serial])

    # Generate an instance uuid, since these versions don't have one.
    instance_uuid = uuid.uuid4()

    # Date XML uploaded to DCC
    day   = int(tree.xpath('//admin:day_of_dcc_upload',   namespaces = root.nsmap)[0].text)
    month = int(tree.xpath('//admin:month_of_dcc_upload', namespaces = root.nsmap)[0].text)
    year  = int(tree.xpath('//admin:year_of_dcc_upload',  namespaces = root.nsmap)[0].text)
    dcc_submission_date = datetime.date(year, month, day)

    try:
        project_code = tree.xpath('//admin:project_code', namespaces = root.nsmap)[0].text
    except IndexError:
        project_code = 'TCGA'

    disease_code  = tree.xpath('//admin:disease_code', namespaces = root.nsmap)[0].text

    # TSS ID
    tss_id    = tree.xpath('//shared:tissue_source_site', namespaces = root.nsmap)[0].text

    # Patient ID, Patient UUID
    case_id   = tree.xpath('//shared:patient_id', namespaces = root.nsmap)[0].text
    case_uuid = tree.xpath('//shared:bcr_patient_uuid', namespaces = root.nsmap)[0].text

    #print ("Root: %s" % etree.tostring(self.tree))
    xml = etree.tostring(tree, encoding='unicode')

    new_dict = {
        'dcc_submission_date' : dcc_submission_date,
        'project_code'        : project_code,
        'disease_code'        : disease_code,
        'batch'               : batch,
        'revision'            : revision,
        'instance_uuid'       : instance_uuid,
        'tss_id'              : tss_id,
        'case_id'             : case_id,
        'case_uuid'           : case_uuid,
        'schema_version'      : schema_version,
        'xml_type'            : xml_type,
        'xml'                 : xml,
            }
    return new_dict

def parse_27_tree(self, tree):
    # XML UUID
    # self.XMLinstanceID = tree.xpath('//shared:instance_id', namespaces = root.nsmap )
    pass

class BadBcrXML(Exception):
    """Some sort of error with format of the XML"""
    def __init__(self, message):
        self.message = message

# Module test
if __name__ == '__main__':

    print("Testing BcrXml object making")
    test_clinical_xml_file_name     = "./nationwidechildrens.org_clinical.TCGA-G2-A2EF.1.xml"
    test_biospecimen_xml_file_name  = "./nationwidechildrens.org_biospecimen.TCGA-G2-A2EF.xml"

    test_clinical_xml_file_handle = open(test_clinical_xml_file_name ,'r')
    test_clinical_xml             = test_clinical_xml_file_handle.read()
    #test_clinical_xml = test_clinical_xml.encode('utf-8')
    test_clinical_xml_file_handle = test_clinical_xml_file_handle.close()

    # Will test as dictionary params, xml string, file handle, and file name
    test_list = [test_clinical_xml_file_name, test_clinical_xml, test_biospecimen_xml_file_name]

    # Get a BCR XML object factory
    factory = BcrXmlClassFactory()
    i = 0
    for test in test_list:
        i += 1
        print("Testing type %s: %s" % (i, type(test)))
        #print("Testing value: %s" % (test))
        try:
            test_BCR_XML_object = factory.make_new_bcr_xml_object(test)
        except BadBcrXML as e:
            print("EXCEPTION2: %s" % repr(e))
            exit()
        except OSError as e:
            print("EXCEPTION3: %s" % repr(e))
            exit()
        except Exception as e:
            print("EXCEPTION4: %s" % repr(e))
            exit()

        # Dump a text representation of the important XML instance tags
        print ("Type %d: %s" % (i, type(test_BCR_XML_object)))
        print ("Dict %d: %s" % (i, test_BCR_XML_object.get_all_but_xml()))
