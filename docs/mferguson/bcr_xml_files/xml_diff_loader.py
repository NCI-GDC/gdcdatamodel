'''
Created on Apr 18, 2014

@author: martin
'''
from bcr_xml import * 

import glob
import psycopg2

from bcr_xml_postgres_interface import PostgresClinXmlGetter
from xml_diff_interface import XmlDiffClass
from subprocess import CalledProcessError
from lxml import etree  # @UnresolvedImport

if __name__ == '__main__':
    
    db_getter = PostgresClinXmlGetter(True)    # True = use the test tables

    # Get a list of all the case_ids
    all_case_ids = db_getter.get_all_case_list()
    #all_case_ids = ['8679']
    
    print ("Total cases to diff: %d" % len(all_case_ids))
    #print (all_case_ids)

    # Open a DB connection. @TODO: get rid of this from here; put into postgres interface class.
    try:
        conn = psycopg2.connect("dbname='BCR_xml' user='TCGAuser' host='knowitall.meshferguson.local' password='tcga-ster'")
    except:
        print("Unable to connect to DB")
        exit(1)
        
    cur = conn.cursor()
    i = 0; j = 0; k = 0; l = 0; m = 0; n = 0
    for case_id in all_case_ids:
        i += 1
#         if (i > 20):
#             break
        print("Case %d: %s" % (i, case_id))
        
        # Returns a list of 2 case objects: last and penultimate.
        xmls = db_getter.get_case_last2_rev(case_id)
        #print(xmls[0].get_xml())
        #print(xmls[1].get_xml())
        
        # Try to generate a difference XML for those 2 cases.
        try:
            # differ = XmlDiffClass(xml_cur, xml_prior)
            differ = XmlDiffClass(xmls[0].get_xml(), xmls[1].get_xml())
        except CalledProcessError as e:
            print ("Fail new XmlDiffClass object: diffx process. Case: %s Except: %s" % (case_id, e))
            j += 1
            continue
        except Exception as e:
            print ("Fail new XmlDiffClass object: something? Case: %s Except: %s" % (case_id, e))
            k += 1
            continue
        
        # Counter: got good diff xml
        l += 1
        
        # Insert the diff XML into the row of the later case.
        # Get a unique row ID for the last case: instance_uuid
        instance_uuid = (xmls[0].get_all_but_xml())['instance_uuid']
        print ("Instance UUID: %s" % instance_uuid)
        print (differ.get_diff_tree())
        
        # Insert the diff XML into the database. This will replace any existing diff XML
        sql = '''UPDATE clinical_xml_test SET diff_xml = %s WHERE (instance_uuid = %s)'''
        #sql = '''UPDATE clinical_xml_test SET diff_xml = %s WHERE (instance_uuid = %s AND diff_xml IS NULL)'''
        
        # Get the XML, use as parameterized argument to SQL statement
        xml = "%s" % differ.get_diff_tree()
        
        try:
            # Always let the library sub the parameters '%s" with data - DON'T do it above!
            cur.execute(sql, (xml, instance_uuid))
        except Exception as e:
            print("Unable to insert into DB")
            print(e)
            m += 1
        finally:
            conn.commit()
        
        # Count
        n += 1

    # Close the DB connection
    conn.close()
    
    print ("Total cases to diff:                     %d" % len(all_case_ids))
    print ("  Total cases failed diff process error:   %d" % (j))
    print ("  Total cases failed diff something else:  %d" % (k))
    print ("  Total cases diff success:                %d" % (l))
    print ("Total diffs to load into DB:             %d" % (l))
    print ("  Total cases diff load fail:              %d" % (m))
    print ("  Total cases diff load success:           %d" % (n))

    
    