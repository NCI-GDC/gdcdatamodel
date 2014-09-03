'''
Created on Apr 13, 2014

@author: martin

This module has 1 purpose: it loads XML into the DB only if those XML files' batch.reve
are not already in the DB. It does a multistep proces:
    1) It peruses the filesystem, using the dir/filename structure historically associated with TCGA,
    and uses the dir/filename as a key to know if the XML is already in the DB.
    2) If not, it still has to check batch.rev, because a later batch.rev in the dir/filename
    could actually hold an earlier batch.rev that was promoted forward if there was no change.

'''

# Get my bcr xml object factory module
from bcr_xml_factory import BcrXmlClassFactory
# Get my bcr_xml class module
from bcr_xml import *
# Get my bcr_xml DB and Filesystem interfaces
from bcr_xml_postgres_interface import PostgresClinXmlGetter, PostgresClinXmlPutter
from bcr_xml_filesystem_interface import FilesystemClinXmlGetter

import psycopg2 # @UnresolvedImport
import psycopg2.extras
import sys

DEF_BASE_DIR    = '/home/martin/my_documents/consulting_practice/NCI/TCGA/05_dcc/all'

if __name__ == '__main__':

    # Create some class instances
    fs_getter = FilesystemClinXmlGetter()
        # This object interfaces to the filesystem to get XML instances
    db_getter = PostgresClinXmlGetter(True) # True use test tables
        # This object does read-only interfaces to the Postgres sql database
    db_putter = PostgresClinXmlPutter(True)
        # This object will new XML instance rows to the Postgres sql database
    factory = BcrXmlClassFactory()
        # This factory object is responsible for returning a dict with complete Postgres row information
        # as need to insert an XML instance. It is supposed to figure out which XML XSD version is being used.

    # 1: Go to the directory. Get a glob of all xml filenames (dir/file)
    dir_filenames = fs_getter.get_list_of_xml_files(fname_filter = '*Level_1.*/*clinical*.xml')
    print("Number of files in FS: %s" % len(dir_filenames))
    #print(dir_filenames[0])

    # 2: Read from the DB. Get list of files (using dir/filename) already in the DB.
    dir_filenames_in_DB = db_getter.get_list_of_xml_files_in_DB()
    print("Number of dir/filenames in DB: %s" % len(dir_filenames_in_DB))
    #print(dir_filenames_in_DB[0])

    # 3.1: Subtract lists: get list of files (dir/filename) in the FS, but NOT in DB. Remember,
    # this list still needs to be checked that its not in the DB by batch.rev (step 3.2).
    s = set(dir_filenames_in_DB)
    dir_filenames_not_in_DB = [x for x in dir_filenames if x not in s]
    print("Number of dir/filenames NOT in DB: %s" % len(dir_filenames_not_in_DB))
    #print(dir_filenames_not_in_DB[0])

    # Now make each dir/filename reference a fully qualified FS location for the file
    files_not_in_DB = [DEF_BASE_DIR + '/' + x for x in dir_filenames_not_in_DB]

    # 3.2: Now each file needs to be factory-ized to get its internal batch.rev. Then internal
    # batch.rev needs to be checked against DB too, and those already in DB subtracted.
    # Iterate through each dir/filename not in DB
    i = 0; j = 0; k = 0; l = 0; m = 0; n =0; p = 0
    for file in files_not_in_DB:
        i += 1
        print ("%d: %s" % (i, file), end='\r')
        try:
            bcr_xml_object = factory.make_new_bcr_xml_object(file)
        except Exception as e:
            print("\n\t%d: Obj factory exception: %s" % (i, e))
            k += 1
            continue
        j +=1
        fourpart_key = [bcr_xml_object.get_disease_code(), bcr_xml_object.get_case_id(), bcr_xml_object.get_batch(), bcr_xml_object.get_revision()]
        #print(batch_rev)

        # Check if this batch.rev exists in DB. Get next file if there already.
        if (db_getter.check_exists_4part_key(fourpart_key)):
            l += 1
            continue
        m += 1

        # 4: The current file is not in by dir/filename, nor by cancer/case_id/batch/rev, so
        # load it into DB.
        try:
            db_putter.put_bcr_xml_object(bcr_xml_object)
        except Exception as e:
            n += 1
            print("\n%d: DB put exception: $s" % (i, e))
            continue

        p += 1

    print()
    print("Number of files in FS scan:                  %s" % len(dir_filenames))
    print("Number of dir/filenames in DB:               %s" % len(dir_filenames_in_DB))
    print("  Number of FS scan dir/filenames NOT in DB:   %s" % len(dir_filenames_not_in_DB))
    print("Files to review for load:                    %d" % (i))
    print("  Review files objectified:                    %d"  % (j))
    print("  Review files failed objectification:         %d" % (k))
    print("Objectified files to get DB batch.rev:       %d" % (j))
    print("  Obj files already in DB by batch.rev:        %d" % (l))
    print("  Obj files not in DB by batch.rev:            %d" % (m))
    print("Files to load into DB:                       %d" % (m))
    print("  Files failed to load into DB:                %d" % (n))
    print("  Files success load into DB:                  %d" % (p))


#         if (i > 15):
#             break

    # Close the DB connection
    #conn.close()
