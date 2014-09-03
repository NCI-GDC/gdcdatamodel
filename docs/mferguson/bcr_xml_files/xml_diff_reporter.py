'''
Created on Apr 21, 2014


@author: martin
'''

import glob
import psycopg2
from collections import defaultdict

from bcr_xml import BcrClinXmlClass
from bcr_xml_postgres_interface import PostgresClinXmlGetter
from xml_diff_interface import XmlDiffClass
from subprocess import CalledProcessError
from lxml import etree  # @UnresolvedImport
from lxml.etree import tostring  # @UnresolvedImport



if __name__ == '__main__':
    
    # Created a nested dictionary the size of our CSV dump pattern
    rec_dd = lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))))
    dump_dict = rec_dd()
    
    # Get a DB interface object
    db_getter = PostgresClinXmlGetter(True)
    all_case_ids = db_getter.get_last_cases_withdiff_list()
    #all_case_ids = ['8679', '1662', '6780']
    #all_case_ids = ['8679']
    #all_case_ids = ['6780']
    print ("Total cases to diff: %d" % len(all_case_ids))
    #print (all_case_ids)
    i = 0
    case_delta = {}
    for case_id in all_case_ids:
        case_delta[case_id] = False
        i += 1
        if (i > 10000):
            break
        my_case       = db_getter.get_case_last_rev(case_id)
        my_case_dict  = my_case.get_all_but_xml()
        cancer        = my_case_dict['disease_code']
        batch         = my_case_dict['batch']
        case_id       = my_case_dict['case_id']
        instance_uuid = my_case_dict['instance_uuid']
        
        #print ("Count %d" % (i))
        #print ("Cancer: %s Batch: %s Case: %s" % (cancer, batch, case_id))
        #print ("\tCase dict: %s" % my_case_dict)
        #print ("XML: %s" % (my_case_dict['diff_xml']))
    
        if (my_case_dict['diff_xml'] is None):
            continue
        diff_tree = etree.fromstring(my_case_dict['diff_xml'])
        trees = {}
        admin_delements     = diff_tree.xpath('//admin:admin//*[dfx:*]', namespaces = diff_tree.nsmap)
        trees['admin']      = admin_delements
        #
        #patient_delements   = diff_tree.xpath('//*[local-name()="patient" and not(descendants::rx:drug)]//*[dfx:del]', namespaces = diff_tree.nsmap)
        patient_delements   = diff_tree.xpath('//*[local-name()="patient"]/*[dfx:*]', namespaces = diff_tree.nsmap)
        trees['patient']    = patient_delements
        #
        followup_delements  = diff_tree.xpath('//*[local-name()="patient"]/*[local-name()="follow_ups"]//*[dfx:del]', namespaces = diff_tree.nsmap)
        trees['follow_ups'] = followup_delements
        #
        cqcf_delements      = diff_tree.xpath('//*[local-name()="patient"]/*[local-name()="clinical_cqcf"]//*[dfx:ins]', namespaces = diff_tree.nsmap)
        trees['cqcf']       = cqcf_delements

        #for ns in ['admin', 'patient', 'follow_ups', 'cqcf']:
        for ns in ['admin', 'patient', 'cqcf', 'follow_ups',]:    
            for delement in trees[ns]:
                if (ns != 'admin'):
                    case_delta[case_id] = True
                tag    = delement.tag.split('}')[1]
                delete = ' '.join(map(str, [x.text   for x in delement.xpath('dfx:del', namespaces = diff_tree.nsmap) if x.text is not None]))
                if (delete == ''):
                    delete = ' '.join(map(str, [x.attrib.get('procurement_status', default = None) for x in delement.xpath('dfx:del', namespaces = diff_tree.nsmap) if x.text is None]))
                    
                insert = ' '.join(map(str, [x.text   for x in delement.xpath('dfx:ins', namespaces = diff_tree.nsmap) if x.text is not None]))
                if (insert == ''):
                    insert = ' '.join(map(str, [x.attrib.get('procurement_status', default = None) for x in delement.xpath('dfx:ins', namespaces = diff_tree.nsmap) if x.text is None]))

                #print("\tNS: %s Tag: %s Del: %s Ins: %s" % (ns, tag, delete, insert))
                dump_dict[cancer][batch][case_id][ns]['tag'].extend([tag])
                dump_dict[cancer][batch][case_id][ns]['del'].extend([delete])
                dump_dict[cancer][batch][case_id][ns]['ins'].extend([insert])
        
    #print (dump_dict)
    # Now dump the list in organized .tsv fashion  
    column_headers = ['Cancer', 'Batch', 'CaseID', 'Delta', 'Domain', 'Changes:']
    print('\t'.join(column_headers))
    for ca in dump_dict.keys():
        for ba in dump_dict[ca].keys():
            for ci in dump_dict[ca][ba].keys():
                #print ('\t'.join(map(str, (dump_dict[ca][ba][ci]['admin']['del'][0:1] + dump_dict[ca][ba][ci]['admin']['ins'][0:1]))))
                for ns in dump_dict[ca][ba][ci].keys():
                    #print("%s\t%s\t%s\t%s" % (ca, ba, ci, ns), end = '\t')
                    if (ns == 'admin'):
                        print("%s\t%s\t%s\t%s\t%s" % (ca, ba, ci, case_delta[ci], ns), end = '\t')
                        print("%s\t%s" % ('batch.rev:', '\t'.join(map(str, (dump_dict[ca][ba][ci]['admin']['del'][0:1] + dump_dict[ca][ba][ci]['admin']['ins'][0:1])))))
                    else:
                        for val in ['tag', 'del', 'ins']:
                            print("%s\t%s\t%s\t%s\t%s\t%s" % (ca, ba, ci, case_delta[ci], ns, val), end = '\t')
                            print ('\t'.join(map(str, dump_dict[ca][ba][ci][ns][val])))
#                     

    