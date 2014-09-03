'''
Created on Apr 15, 2014

@author: martin
'''

from bcr_xml import * 
import psycopg2
import psycopg2.extras
from xml_diff_interface import *

class PostgresClinXmlGetter(object):
    '''
    classdocs
    '''

    def __init__(self, test):
        '''
        Constructor
        '''
        if (test is True):
            self.table = 'clinical_xml_test'
        else:
            self.table = 'clinical_xml'
            
        try:
            self.conn = psycopg2.connect("dbname='BCR_xml' user='TCGAuser' host='knowitall.meshferguson.local' password='tcga-ster'")
        except:
            print("Unable to connect to DB")
            exit(1)

    def __del__(self):

        # General method to close the DB connection
        self.conn.close()

    def get_case_last_rev(self, case_id):
        last_case = self.get_case_rev(case_id, 1)
        return last_case.pop()
    
    def get_case_last2_rev(self, case_id):
        return self.get_case_rev(case_id, 2)
    
    def get_case_rev(self, case_id, count):
        
        dict_cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        sql = '''SELECT * FROM %s ''' % (self.table) + '''WHERE (case_id = %s) ORDER BY revision DESC LIMIT %s'''
        dict_cursor.execute(sql, (case_id, count))
        # Each result is a dictionary of every column name : value
        results = dict_cursor.fetchall()
        dict_cursor.close()
        
        bcr_xml_list = []
        for result in results:
            # Create list of BCR_xml objects
            bcr_xml_list.append(BcrClinXmlClass(result))
            
        return bcr_xml_list
    
    def get_all_case_list(self):
        cursor = self.conn.cursor()
        sql = '''SELECT DISTINCT case_id FROM %s''' % (self.table)
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        
        return [i[0] for i in results] # Convert list of single value tuples into list

    def get_last_cases_withdiff_list(self):
        '''
        This function returns a list of case_ids where the last case_id has a diff_xml.
        '''
        cursor = self.conn.cursor()
        #sql = '''SELECT DISTINCT case_id, revision, diff_xml FROM %s ''' % (self.table) + '''WHERE (diff_xml IS NOT NULL) ORDER BY revision DESC LIMIT 1'''
        sql = '''select a.case_id from clinical_xml_test a where a.diff_xml is not null and a.revision = (select max(b.revision) from clinical_xml_test b where a.case_id = b.case_id)'''
        #cursor.execute(sql, (args))
        cursor.execute(sql)
        results = cursor.fetchall()
        results = [x[0] for x in results]
        cursor.close()
        
        return results
    
    def get_list_of_xml_files_in_DB(self):
        '''
        This method returns a list of the directory/filename strings for each XML file in the DB.
        '''
        cursor = self.conn.cursor()
        sql = '''SELECT DISTINCT dir_fname_location FROM clinical_xml_test;'''
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        
        return [i[0] for i in results] # Convert list of single value tuples into list
    
    def check_exists_4part_key(self, fourpart_key):
        '''
        This method checks if a 4 part unique key for a BCR XML instance exists already in the database.
        Key parts are disease_code, case_id, batch, revision
        '''
        cursor = self.conn.cursor()
        sql = 'SELECT TRUE FROM clinical_xml_test WHERE (disease_code = %s AND case_id = %s AND batch = %s AND revision = %s);'
        try:
            cursor.execute(sql, (fourpart_key))
        except Exception as e:
            return False
#                 
        results = cursor.fetchone()
        cursor.close()
        return results
    

class PostgresClinXmlPutter(object):
    '''
    Class that puts things into the database
    '''

    def __init__(self, test):
        '''
        Constructor
        '''
        if (test is True):
            self.table = 'clinical_xml_test'
        else:
            self.table = 'clinical_xml'
            
        try:
            self.conn = psycopg2.connect("dbname='BCR_xml' user='TCGAuser' host='knowitall.meshferguson.local' password='tcga-ster'")
        except:
            print("Unable to connect to DB")
            exit(1)

    def __del__(self):

        # General method to close the DB connection
        self.conn.close()

    def put_bcr_xml_object(self, bcr_xml_object):
        
        data_array = bcr_xml_object.get_all()
        cursor = self.conn.cursor()
        # Enable psycopg deal with UUIDs
        psycopg2.extras.register_uuid()
        
        # Create a parameterized SQL string with the number of value '%s's = to length of the dictionary
        sql = '''INSERT INTO %s ''' % (self.table) + '''(%s) VALUES(%s)''' % (','.join(data_array.keys()), ', '.join(['%s'] * len(data_array)))
        # Creates something like: "INSERT INTO clinical_xml (xml_type,batch,...) VALUES (%s, %s, %s, ...)"
        
        try:
            # Always let the library sub the parameters '%s" with data - DON'T do it above!
            cursor.execute(sql, tuple(data_array.values()))
        except Exception as e:
            print("DB Exception: %s Object: %s" % (e, data_array))
        finally:
            self.conn.commit()



        
    def put_case_diffxml(self, case_id, diff_xml):
        cursor = self.conn.cursor()
        sql = '''INSERT INTO %s ''' % (self.table) + '''(case_id, diff_xml) VALUES(%s, %s)'''
        
        try:
            cursor.execute(sql, (case_id, diff_xml))
        except:
            pass
        
        cursor.close()
#########################################################################################3
if __name__ == '__main__':
    
    # First, get some stuff
    test_getter = PostgresClinXmlGetter(True);
    case_id = 'A2EF'

    # Get 1 case
    result = test_getter.get_case_last_rev(case_id)
    print ("Type: %s Result: %s Value: %s" % (type(result), result, result.get_all_but_xml()))
    print ("XML: %s" % result.get_xml())
       
    # Get 2 cases       
    results = test_getter.get_case_last2_rev(case_id)
    for result in results:
        print ("Type: %s Result: %s Value: %s" % (type(result), result, result.get_all_but_xml()))
        print ("XML: %s" % result.get_all_but_xml())
    print("Case count: %s" % len(results))
    
    # Generate a difference XML on 2 test cases
    #print (results[0].get_xml())
    diffs = XmlDiffClass(results[0].get_xml(), results[1].get_xml())
    diff_xml = diffs.get_diff_tree()
    print (diff_xml)
    
    # Put the diff_xml into the record
    
    # Get list of all cases
    all_cases = test_getter.get_all_case_list()
    for case in all_cases:
        print("Case: %s" % case)


   
    