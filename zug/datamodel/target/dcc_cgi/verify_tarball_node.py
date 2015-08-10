#!/usr/bin/env python

import os,sys
import requests
from requests.auth import HTTPBasicAuth
import base64
import getpass
from bs4 import BeautifulSoup
import urllib3
import logging
import datetime, time
import json
import shutil
from lxml import html
from tarstream import TarStream, Stream
from psqlgraph import PsqlGraphDriver
from gdcdatamodel import models as mod
from signpostclient import SignpostClient
from s3_wrapper import S3_Wrapper
import md5

urllib3.disable_warnings()
logging.captureWarnings(True)

class TargetDCCCGIVerifier(object):

    def __init__(self):
        self.count = 0
        self.total_size = 0
        if 'SIGNPOST_URL' in os.environ:
            self.signpost_url = os.environ['SIGNPOST_URL']
        else:
            print "Warning, SIGNPOST_URL not found, defaulting to signpost.service.consul"
            self.signpost_url = "http://signpost.service.consul"
        
        self.pq_creds = {}
        self.dcc_creds = {}

        for env in os.environ.keys():
            if env.find('PG_HOST') != -1:
                print "Setting 'host_name' to", os.environ[env]
                self.pq_creds['host_name'] = os.environ[env]
            if env.find('PG_USER') != -1:
                print "Setting 'user_name' to", os.environ[env]
                self.pq_creds['user_name'] = os.environ[env]
            if env.find('PG_NAME') != -1:
                print "Setting 'db_name' to", os.environ[env]
                self.pq_creds['db_name'] = os.environ[env]
            if env.find('PG_PASS') != -1:
                print "Setting 'password' to", os.environ[env]
                self.pq_creds['password'] = os.environ[env]
            if env.find('DCC_USER') != -1:
                self.dcc_creds['id'] = os.environ[env]
            if env.find('DCC_PASS') != -1:
                self.dcc_creds['pw'] = os.environ[env]

        # TODO: move these to environment vars/config
        self.target_acl = "phs000471"
        self.target_object_store = "ceph"

        if 'TARGET_PROTECTED_BUCKET' in os.environ:
            self.target_bucket_name = os.environ['TARGET_PROTECTED_BUCKET']
        else:
            print "Warning, TARGET_PROTECTED_BUCKET not found, defaulting to test"
            self.target_bucket_name = "test"


    def connect_to_psqlgraph(self):
        psql = PsqlGraphDriver(
            self.pq_creds['host_name'],
            self.pq_creds['user_name'],
            self.pq_creds['password'],
            self.pq_creds['db_name'])

        return psql

    def get_idpw(self):
        username = raw_input('Username: ')
        passwd = getpass.getpass('Password: ')

        return { 'id': username, 'pw': passwd }

    def unpack_size(self, size_str):
        size = 0
        size_table = {
            "K": 1000,
            "M": 1000000,
            "G": 1000000000,
            "T": 1000000000000,
            "P": 1000000000000000 }

        if size_str[-1].isalpha():
            size = float(size_str[:-1]) * size_table[size_str[-1]]
        else:
            size = int(size_str)

        return size

    def get_nearest_file_size(self, size):
        sizes = [
            (1000000000000000000, "PB"),
            (1000000000000000, "TB"),
            (1000000000000, "GB"),
            (1000000000, "MB"),
            (1000000, "KB"),
            (1000, "KB") ]
         
        value = sizes[len(sizes) - 1]
        for entry in sizes:
             if size < entry[0]:
                continue
             value = entry
             break
        
        #print "%d bytes, for %s" % (size, value[1])
        return value

    def verify_tarball(self, tarball_name, project_name):

        results = { 
            'result': 'unknown', 
            'reason': 'unknown'}

        pq = self.connect_to_psqlgraph()
        with pq.session_scope() as session:
            # find the main node
            tarball_nodes = pq.nodes(mod.File).props(file_name=tarball_name).all()
            if (tarball_nodes != None) and (len(tarball_nodes) != 0):
                if len(tarball_nodes) > 1:
                    print "More than one node found with name %s" % tarball_name
                    for node in tarball_nodes:
                        print node.node_id
                    results['result'] = "failed"
                    results['reason'] = "more than one tarball node found"
                else:
                    tarball_node = tarball_nodes[0]
                    # verify the information
                    print "tarball id:", tarball_node.node_id
                    print "tarball props:", tarball_node.props
                    print "tarball sysan:", tarball_node.system_annotations

                    if tarball_node.props['submitter_id'] != project_name:
                        print "Incorrect submitter_id:", tarball_node.props['submitter_id']
                        results['result'] = "failed"
                        results['reason'] = "incorrect submitter_id"
                    # check the file size
                    
                    all_edges = tarball_node.get_edges()
                    for edge in all_edges:
                        if edge.dst.node_id != tarball_node.node_id:
                            print "Edge:", edge.dst.label, edge.dst.props, edge.dst.sysan
                        else:
                            print "Edge:", edge.src.label, edge.src.props, edge.dst.sysan

            else:
                print "Unable to find file %s" % tarball_name
                results['result'] = "failed"
                results['reason'] = "tarball node not found"


        return results


if __name__ == '__main__':
    tdc_vr = TargetDCCCGIVerifier()
    tdc_vr.verify_tarball(sys.argv[1], sys.argv[2])
