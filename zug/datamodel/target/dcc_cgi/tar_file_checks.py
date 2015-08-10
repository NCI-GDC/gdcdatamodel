#!/usr/bin/env python

import os,sys
import requests
import getpass
from tarstream import TarStream, Stream
import tarfile
from s3_wrapper import S3_Wrapper
import md5

root_url = "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/"

# https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/manifest.all.unencrypted
files = [
    "TARGET-50-CAAAAH/EXP/manifest.all.unencrypted",
    "TARGET-50-CAAAAH/EXP/README.2.1.0.txt",
    "TARGET-50-CAAAAH/EXP/manifest.all.unencrypted.sig",
    "TARGET-50-CAAAAH/EXP/manifest.dcc.unencrypted", 
    "TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-01A-01D/ASM/somaticVcfBeta-GS000010157-ASM-T1-N1_maf_FET_C13.txt" ]
#    "TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-01A-01D/ASM/EVIDENCE-GS000010157-ASM-N1/evidenceDnbs-chr1-GS000010157-ASM-T1.tsv.bz2"]
#    "TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-01A-01D/ASM/masterVarBeta-GS000010157-ASM-T1-N1.tsv.bz2"]

def get_idpw():
    username = raw_input('Username: ')
    passwd = getpass.getpass('Password: ')

    return { 'id': username, 'pw': passwd }

def test_tarball_stream_upload(root_url, file_list, auth_data, bucket_name, s3_inst):
    tests = {}
    passed = True
    s3_conn = s3_inst.connect_to_s3("ceph")
    tarball_name = "test_tarball.tar.gz"
    tarball_key_name = "test_dir/" + tarball_name
    streams = []
    file_name_list = []
    for entry in file_list:
        streams.append(Stream(root_url + entry, entry, auth_data, calc_md5=True))
        entry.split

    # create the stream for the tarball and stream it to S3
    tar_ball = TarStream(streams, tarball_name)

    tar_ball_results = s3_inst.upload_multipart_file(s3_conn, bucket_name, tarball_key_name, tar_ball, True, True)
    print "Tarball md5 = %s, bytes = %d" % (tar_ball_results['md5_sum'], tar_ball_results['bytes_transferred'])

    # now check the uploaded file to make sure it works

    # download the file to local storage
    file_key = s3_inst.get_file_key(s3_conn, bucket_name, tarball_key_name)
    file_name = file_key.name.split('/')[1]
    print "Downloading %s, size %d" % (file_key.name, file_key.size)
    m = md5.new()
    # download the file
    with open(file_name, "w") as temp_file:
        bytes_requested = 10485760
        chunk_read = file_key.read(size=bytes_requested)
        total_transfer = 0
#        while total_transfer < file_key.size:
#            if len(chunk_read):
        while len(chunk_read):
            m.update(chunk_read) 
            #sys.stdout.write("%d/%d\r" % (total_transfer, file_key.size))
            #sys.stdout.flush()
            temp_file.write(chunk_read)
            total_transfer = total_transfer + len(chunk_read)
            chunk_read = file_key.read(size=bytes_requested)
            #else:
            #    print "0 bytes read"

    if total_transfer == file_key.size:
        print "Read expected number of bytes"
    else:
        print "Read %d bytes, expecting %d" % (total_transfer, file_key.size)

    if m.hexdigest() != tar_ball_results['md5_sum']:
        print "FAILED: tarball md5 mismatch"
        print "expected: %s" % tar_ball_results['md5_sum']
        
        print "     got: %s" % m.hexdigest()
        passed = False
        tests['tarball_md5'] = "failed"
    else:
        tests['tarball_md5'] = "passed"

    for entry in tar_ball.streams:
        print entry.name, entry.size, entry.get_md5()

    # see if it's a valid tarball
    if tarfile.is_tarfile(file_name):
        tests['tarball_check'] = "passed"
        tb = tarfile.open(file_name)
        try:
            names = tb.getnames()
        except:
            print "ERROR: unable to get names from tarball"
            tests['tarball_check'] = "failed"
        else:
            if len(names) == len(file_list):
                tests['file_count'] = "passed"
                tests['file_match'] = "passed"
                for entry2 in file_list:
                    if entry2 not in names:
                        print "FAILED: %s not present in tarball" % entry
                        tests['file_match'] = "failed"
                        passed = False
                    else:
                        tar_info = tb.getmember(entry2)
                        tar_member = tb.extractfile(tar_info)
                        for stream in tar_ball.streams:
                            if stream.name == entry2:
                                # make sure the sizes match
                                if stream.size != tar_info.size:
                                    print "FAILED: file size mismatch for %s (expected: %d, got: %d)" % (
                                        entry2, stream.size, tar_info.size)
                                    passed = False
                                else:
                                    # make sure we have proper md5 sums
                                    m2 = md5.new()
                                    chunk = tar_member.read(size=bytes_requested)
                                    while len(chunk):
                                        m2.update(chunk) 
                                        chunk = tar_member.read(size=bytes_requested)
                                    if m2.hexdigest() != stream.get_md5():
                                        print "FAILED: file md5 mismatch for %s (expected: %s, got: %s)" % (
                                            entry2, stream.calc_md5, m2.hexdigest())
                                
            else:
                print "FAILED: file count mismatch, expecting %d, got %d" % (len(file_list), len(names))
                print names
                print file_list
                tests['file_count'] = "failed"
                passed = False
    else:
        print "FAILED: Tarfile is invalid"
        tests['tarball_check'] = "failed"
        passed = False

    # delete the file
    os.remove(file_name)

    # delete the key from the object store
    s3_inst.delete_file_key(s3_conn, bucket_name, tarball_key_name) 

    return passed

#def test_simple_upload():
#    print "Not yet"

#def test_node_edge_creation():

s3_inst = S3_Wrapper()

s3_bucket_name = "test"

auth_data = get_idpw()

passed = test_tarball_stream_upload(root_url, files, auth_data, "test", s3_inst)
if passed:
    print "Test passed"
else:
    print "Test failed"


