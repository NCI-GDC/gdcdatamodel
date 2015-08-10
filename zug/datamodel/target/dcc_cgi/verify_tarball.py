#!/usr/bin/env python
from s3_wrapper import S3_Wrapper
import os, sys
import tarfile

s3_inst = S3_Wrapper()
s3_conn = s3_inst.connect_to_s3("ceph")
s3_bucket = "test"

# parse the args

# find the key 

# download the file
file_info = s3_inst.download_file(s3_conn, s3_bucket, sys.argv[1])
file_name = sys.argv[1].split('/')[-1]
if tarfile.is_tarfile(file_name):
    tb = tarfile.open(file_name)
    try:
        names = tb.getnames()
    except:
        print "ERROR: unable to get names from tarball"
    else:
        for name in names:
            print name
        # get the manifest file
        try:
            tar_info = tb.getmember("TARGET-50-CAAAAA/EXP/manifest.all.unencrypted")
        except:
            print "Unable to retrieve manifest from tarball"
        else:
            tar_member = tb.extractfile(tar_info)
            for line in tar_member.readline():
                print line
                # make sure we have proper md5 sums
                #m2 = md5.new()
                #chunk = tar_member.read(size=bytes_requested)
                #while len(chunk):
                #    m2.update(chunk) 
                #    chunk = tar_member.read(size=bytes_requested)
                #if m2.hexdigest() != stream.get_md5():
                #    print "FAILED: file md5 mismatch for %s (expected: %s, got: %s)" % (
                #        entry2, stream.calc_md5, m2.hexdigest())
                    
else:
    print "FAILED: Tarfile is invalid"
    tests['tarball_check'] = "failed"
    passed = False

