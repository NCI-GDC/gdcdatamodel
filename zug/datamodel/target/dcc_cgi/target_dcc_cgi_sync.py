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
#try: # Python 3.x 
#    import http.client as http_client
#except ImportError: # Python 2.x 
#    import httplib as http_client

#http_client.HTTPConnection.debuglevel = 1

urllib3.disable_warnings()
logging.captureWarnings(True)

class TargetDCCCGIDownloader(object):

    def __init__(self):
        self.strings_to_ignore = [ 
            "Name", "Last modified", 
            "Size", "Parent Directory", 
            "lost+found/", "Parent Directory" ]
        self.urls_to_check = [
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/PilotAnalysisPipeline2/",
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/" ]
        self.row_classes = [ "even", "odd" ]
        self.count = 0
        self.total_size = 0
        if 'SIGNPOST_URL' in os.environ:
            self.signpost_url = os.environ['SIGNPOST_URL']
        else:
            print "Warning, SIGNPOST_URL not found, defaulting to signpost.service.consul"
            self.signpost_url = "http://signpost.service.consul"

        cur_time = datetime.datetime.now()
        self.log_filename = "/home/ubuntu/logs/added_target_dcc_cgi_files_%04d-%02d-%02d_%02d-%02d-%02d.json" % (
            cur_time.year, cur_time.month, cur_time.day,
            cur_time.hour, cur_time.now().minute, cur_time.second)
        self.pq_creds = {}
        self.dcc_creds = {}
        self.log_data = []
        self.logged_keys = []

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
        #self.target_acls = ["phs000471", "phs000218"]
        self.target_acls = []
        self.target_object_store = "ceph"

        if 'TARGET_PROTECTED_BUCKET' in os.environ:
            self.target_bucket_name = os.environ['TARGET_PROTECTED_BUCKET']
        else:
            print "Warning, TARGET_PROTECTED_BUCKET not found, defaulting to test"
            self.target_bucket_name = "test_2"


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

    def get_nearest_file_size(size):
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

    def write_json_to_file(self, data):
        with open(self.log_filename, "a") as log_file:
            json.dump(data, log_file)
            log_file.write("\n")

    def process_tree(self, url, auth_data, url_list, test_download=False):
        #print "Walking %s in %s" % (url, cur_dir)
        r = requests.get(url, auth=(auth_data['id'], auth_data['pw']), verify=False)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text)
            file_table = soup.find('table', attrs={'id':'indexlist'})
            rows = file_table.find_all('tr')
            for row in rows:
                if row['class'][0] in self.row_classes:
                    image = row.find('img')
                    # directory
                    if image['alt'].find("[DIR]") != -1:
                        dir_name = row.find('td', class_="indexcolname").get_text().strip()
                        self.process_tree(url + dir_name, auth_data, url_list)
                    # file
                    else:
                        if image['alt'].find("DIR") == -1:
                            file_name = row.find('td', class_="indexcolname").get_text().strip()
                            link = row.find('a')
                            file_url = url + link['href']
                            if test_download == False:
                                url_list.append(file_url)
                            else:
                                print "Downloading file: %s to %s from %s" % (file_name, os.getcwd(), file_url)
                                open(file_name, 'a').close()

    # This routine isn't used, but it might be handy to have someday, so leaving it in here for now
    def download_tree(self, url, auth_data, cur_dir, test_download=False):
    #    print "Walking %s in %s" % (url, os.getcwd() + "/" + cur_dir)
        print "Walking %s in %s" % (url, cur_dir)
        if not os.path.exists(cur_dir):
            os.makedirs(cur_dir)
        os.chdir(cur_dir)
        r = requests.get(url, auth=(auth_data['id'], auth_data['pw']), verify=False)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text)
            file_table = soup.find('table', attrs={'id':'indexlist'})
            rows = file_table.find_all('tr')
            for row in rows:
                if (row['class'][0] == "even") or (row['class'][0] == "odd"):
                    image = row.find('img')
                    # directory
                    if image['alt'].find("[DIR]") != -1:
                        dir_name = row.find('td', class_="indexcolname").get_text().strip()
                        if not os.path.exists(dir_name):
                            os.makedirs(dir_name)
                        print "Calling download_tree with %s in %s" % (url + dir_name, dir_name)
                        self.download_tree(url + dir_name, auth_data, dir_name, test_download)
                    # file
                    else:
                        if image['alt'].find("DIR") == -1:
                            file_name = row.find('td', class_="indexcolname").get_text().strip()
                            link = row.find('a')
                            file_url = url + link['href']
                            if test_download == False:
                                dr = requests.get(file_url, auth=(auth_data['id'], auth_data['pw']), stream=True, verify=False)
                                if dr.status_code == 200:
                                    with open(file_name, 'wb') as f:
                                        shutil.copyfileobj(dr.raw, f)
                                else:
                                    print "Download failed:", dr.status_code, dr.reason
                            else:
                                print "Downloading file: %s to %s from %s" % (file_name, os.getcwd(), file_url)
                                open(file_name, 'a').close()

    def get_directory_list(self, url, auth_data):
        directory_list = []
        r = requests.get(url, auth=(auth_data['id'], auth_data['pw']), verify=False)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text)
            file_table = soup.find('table', attrs={'id':'indexlist'})
            rows = file_table.find_all('tr')
            for row in rows:
                if (row['class'][0] == "even") or (row['class'][0] == "odd"):
                    image = row.find('img')
                    if image['alt'].find("[DIR]") != -1:
                        dir_data = {}
                        dir_data['dir_name'] = row.find('td', class_="indexcolname").get_text().strip()
                        link = row.find('a')
                        dir_data['url'] = url + link['href']
                        directory_list.append(dir_data)
        else:
            print r.status_code, r.reason

        return directory_list

    def create_signpost_entry(self):
        print ""

    def create_signpost_uri(self, object_store, bucket, key_name):
        return "s3://%s/%s/%s" % (object_store, bucket, key_name)

    def create_tarball_file_node(
        self, pq, signpost, 
        tarball_name, tarball_md5_sum, tarball_size, 
        s3_key_name, project, participant_barcode):

        tarball_node_id = "0"
        file_node = None
        node_exists = False

        # see if we exist before getting a new ID
        results = pq.nodes(mod.File).props(file_name=tarball_name).all()
        if len(results) == 0:
            print "New file, registering with gdcapi"
        else:
            if len(results) > 1:
                print "More than one file found with that name, quitting"
                node_exists = True
                sys.exit()
            else:
                print "File exists, verifying/modifying that id"
                tarball_node_id = results[0].node_id
                file_node = results[0]
                node_exists = True

        if not node_exists:
            # check if file is in signpost yet
            tarball_node_id = signpost.create().did
            tarball_uri = self.create_signpost_uri(self.target_object_store, self.target_bucket_name, s3_key_name)
            doc = signpost.get(tarball_node_id)
            doc.urls = [tarball_uri]
            doc.patch()
            node_exists = False
            file_properties = {
                'state': "submitted",
                'file_size': tarball_size,
                'md5sum': tarball_md5_sum,
                'file_name': tarball_name,
                'submitter_id': project,
                'state_comment': None
            }

            file_sysan = {
                'source': 'target_dcc_cgi',
                '_participant_barcode': participant_barcode
            }

            file_node = mod.File(
                node_id=tarball_node_id,
                acl=self.target_acls,
                properties=file_properties,
                system_annotations=file_sysan
            )
        else:
            # verify that the signpost ID works
            tarball_uri = self.create_signpost_uri(self.target_object_store, self.target_bucket_name, s3_key_name)
            doc = signpost.get(tarball_node_id)
            # NB: we might not just be able to blow away at a point,
            # but for now, if it exists, just reset to our uri
            doc.urls = [tarball_uri]
            doc.patch()

            # reset all the information
            file_properties = {
                'state': "submitted",
                'file_size': tarball_size,
                'md5sum': tarball_md5_sum,
                'file_name': tarball_name,
                'submitter_id': project,
                'state_comment': None
            }

            file_sysan = {
                'source': 'target_dcc_cgi',
                '_participant_barcode': participant_barcode
            }

            pq.node_update(
                file_node,
                acl=self.target_acls,
                properties=file_properties,
                system_annotations=file_sysan
            )


        return tarball_node_id, file_node

    def create_related_file_node(self, pq, signpost, entry, project, participant_barcode, tarball_file_node):
        if tarball_file_node != None:
            print "Trying to add", entry['file_name']
            add_related_file = True
            related_file_node = None
            # check if the node already has an edge to a file with this name
            for related_file in tarball_file_node.related_files:
                if related_file.file_name == entry['file_name']:
                    print "Related file found", entry['file_name']
                    add_related_file = False
                    related_file_node = related_file

            if add_related_file:
            #results = pq.nodes(mod.File).props(name=entry['file_name']).all()
            #if len(results) == 0:
                print "Adding node to graph for %s" % entry['file_name']

                # get an id from signpost
                assoc_file_node_id = signpost.create().did
                assoc_file_uri = self.create_signpost_uri(
                    self.target_object_store, 
                    self.target_bucket_name, 
                    entry['s3_key_name'])

                # create the node
                file_properties = {
                    'state': "submitted",
                    'md5sum': entry['md5_sum'],
                    'file_size': entry['file_size'],
                    'file_name': entry['file_name'],
                    'submitter_id': project,
                    'state_comment': None
                }

                file_sysan = {
                    'source': 'target_dcc_cgi',
                    '_participant_barcode': participant_barcode
                }

                file_node = mod.File(
                    node_id=assoc_file_node_id,
                    acl=self.target_acls,
                    properties=file_properties,
                    system_annotations=file_sysan
                )

                #print entry
                tarball_file_node.related_files.append(file_node)
                #edge_to_file = mod.FileRelatedToFile(assoc_file_node_id, tarball_node_id)

                #pq.current_session().merge(edge_to_file)
                doc = signpost.get(assoc_file_node_id)
                doc.urls = [assoc_file_uri]
                doc.patch()
            else:
                #if len(results) == 1:
                # check signpost
                assoc_file_uri = self.create_signpost_uri(
                    self.target_object_store, 
                    self.target_bucket_name, 
                    entry['s3_key_name'])
                # NB: we might not just be able to blow away at a point,
                # but for now, if it exists, just reset to our uri
                doc = signpost.get(related_file_node.node_id)
                doc.urls = [assoc_file_uri]
                doc.patch()

                # reset data
                file_properties = {
                    'state': "submitted",
                    'md5sum': entry['md5_sum'],
                    'file_size': entry['file_size'],
                    'file_name': entry['file_name'],
                    'submitter_id': project,
                    'state_comment': None
                }

                file_sysan = {
                    'source': 'target_dcc_cgi',
                    '_participant_barcode': participant_barcode
                }

                pq.node_update(
                    related_file_node,
                    acl=self.target_acls,
                    properties=file_properties,
                    system_annotations=file_sysan
                )

                #else:
                #    print "More than one entry, quitting"
                #    sys.exit()

    def create_edges(self, pq, tarball_node_id, tarball_file_node, project):
        if tarball_file_node != None:
            # platform
            if len(tarball_file_node.platforms) == 0:
                platform_node = pq.nodes(mod.Platform).props(name="Complete Genomics").first()
                if platform_node != None:
                    tarball_file_node.platforms.append(platform_node)
                else:
                    print "Unable to find Platform 'Complete Genomics'"

            # data_subtype
            if len(tarball_file_node.data_subtypes) == 0:
                data_subtype_node = pq.nodes(mod.DataSubtype).props(name="CGI Archive").first()
                if data_subtype_node != None:
                    tarball_file_node.data_subtypes.append(data_subtype_node)
                else:
                    print "Unable to find Data Subtype 'CGI Archive'"

            # data_format
            if len(tarball_file_node.data_formats) == 0:
                data_format_node = pq.nodes(mod.DataFormat).props(name="TARGZ").first()
                if data_format_node != None:
                    tarball_file_node.data_formats.append(data_format_node)
                else:
                    print "Unable to find Data Format 'TARGZ'"

            # experimental_strategy
            if len(tarball_file_node.experimental_strategies) == 0:
                experimental_strategy_node = pq.nodes(mod.ExperimentalStrategy).props(name="WGS").first()
                if experimental_strategy_node != None:
                    tarball_file_node.experimental_strategies.append(experimental_strategy_node)
                else:
                    print "Unable to find Experimental Strategy 'WGS'"

            # tag
            if len(tarball_file_node.tags) == 0:
                tag_node = pq.nodes(mod.Tag).props(name=project).first()
                if tag_node != None:
                    tarball_file_node.tags.append(tag_node)
                else:
                    print "Unable to find Tag '%s'" % project

    def find_latest_log(self, directory):
        cur_dir = os.getcwd()
        os.chdir(directory)
        newest = max(os.listdir("."), key = os.path.getctime)
        os.chdir(cur_dir)
        return directory + "/" + newest

    def get_log_file_data(self, log_file_name):
        file_data = []
        logged_files = []
        with open(log_file_name, "r") as log_file:
            for line in log_file:
                data = json.loads(line)
                if "file_name" in data:
                    file_data.append(data)
                    logged_files.append(data['s3_key_name'])
        return file_data, logged_files

    def process_job(self, directory, object_store, bucket, auth_data, project, re_md5_tarball=True):
        print "Processing", directory['dir_name'], "for project", project, "with tarball recheck=", re_md5_tarball

        create_nodes = True 
        node_data = {}
        url_list = []
        tar_list = []
        download_list = []
        aliquot_submitter_ids = set()
        aliquot_ids = []
        pq = self.connect_to_psqlgraph() 
        link_count = 0

        signpost = SignpostClient(self.signpost_url)

        self.write_json_to_file({
            'directory name':directory['dir_name'],
            'project':project
        })

        tarball_name = "%s.%s.tar.gz" % (directory['dir_name'].strip('/'), project)
        print "tarball:", tarball_name

        node_data['participant_barcode'] = directory['dir_name'].strip("/")

        # parse the tree to find files to archive/download
        self.process_tree(directory['url'], auth_data, url_list)
        print "Tree complete: %s" % directory['url']
        print "%d items in list" % len(url_list)

        # find files to exclude
        for entry in url_list:
            url_parts = entry.split("/")
            if url_parts[-2] == "EXP":
                dl_entry = {}
                dl_entry['url'] = entry
                dl_entry['s3_key_name'] = project + "/" + url_parts[-3] + "/" + url_parts[-1]
                dl_entry['file_name'] = url_parts[-1]
                download_list.append(dl_entry)
            if url_parts[-3] == "EXP":
                aliquot_submitter_ids.add(url_parts[-2])
            tar_list.append(Stream(entry, entry, auth_data))
            #link_count += 1
            #if link_count > (len(url_list) * 0.5):
            #    break
        
        print "%d items in tar list" % len(tar_list)
        print "%d items in download list" % len(download_list)
        for entry in download_list:
            print entry['s3_key_name']
        print "%d aliquot submitter ids found" % len(aliquot_submitter_ids)
        print aliquot_submitter_ids

        # check if the key already exists
        s3_inst = S3_Wrapper()
        s3_conn = s3_inst.connect_to_s3(object_store)
        s3_key_name = "%s/%s%s" % (project, directory['dir_name'], tarball_name) 
        #s3_key_name = tarball_name 
        file_key = s3_inst.get_file_key(s3_conn, bucket, s3_key_name)

        # TODO: if the key does exist, we need to check if the archive needs to be updated
        if file_key == None:
            print "Key %s not found, downloading and creating archive" % s3_key_name

            # create the tarball
            tar_ball = TarStream(tar_list, tarball_name)

            # upload tarball to s3
            #s3_inst = S3_Wrapper()
            #s3_conn = s3_inst.connect_to_s3(object_store)
            s3_key_name = "%s/%s%s" % (project, directory['dir_name'], tarball_name) 
            #s3_key_name = tarball_name
            upload_stats = s3_inst.upload_multipart_file(s3_conn, bucket, s3_key_name, tar_ball, True)
            tarball_md5_sum = upload_stats['md5_sum']
            tarball_size = upload_stats['bytes_transferred']
            print "md5 sum:", tarball_md5_sum, "size:", tarball_size
            self.write_json_to_file({
                'file_name':tarball_name,
                's3_key_name':s3_key_name,
                'md5_sum': tarball_md5_sum,
                'file_size': tarball_size,
            })
        else:
            print "Key %s already found (%d), skipping upload" % (
                file_key.name, file_key.size)
            # md5 the file
            md5_1 = md5.new()
            tarball_size = 0
            bytes_requested = 10485760
            if re_md5_tarball:
                chunk_read = file_key.read(size=bytes_requested)
                while len(chunk_read):
                    tarball_size += len(chunk_read)
                    md5_1.update(chunk_read)
                    chunk_read = file_key.read(size=bytes_requested)

                tarball_md5_sum = md5_1.hexdigest()
            else:
                print "skipping md5 recheck"
                # load the state from the existing log file
                tarball_md5_sum = ""
                tarball_size = -1
                for file_data in self.log_data:
                    if file_data['s3_key_name'] == file_key.name:
                        tarball_md5_sum = file_data['md5_sum']
                        tarball_size = file_data['file_size']
                        break
                if tarball_size == -1:
                    print "Unable to find key %s in log file" % file_key.name

            if tarball_size == file_key.size:
                print "md5 sum:", tarball_md5_sum, "size:", tarball_size
                self.write_json_to_file({
                    'file_name':tarball_name,
                    's3_key_name':s3_key_name,
                    'md5_sum': tarball_md5_sum,
                    'file_size': tarball_size,
                })
            else:
                print "File size mismatch for %s, key reports %d, got %d" % (
                    file_key.name, file_key.size, tarball_size
                )
                sys.exit()

        # transfer the other files to the object store
        for entry in download_list:
            print entry 
            # check if the key exists
            file_key = s3_inst.get_file_key(s3_conn, bucket, entry['s3_key_name']) 
            if file_key == None:
                print "File %s not present, uploading" % entry['s3_key_name']
                file_stream = Stream(entry['url'], entry['url'].split("/")[-1], auth_data)
                file_stream.connect()
                assoc_file_stats = s3_inst.upload_multipart_file(s3_conn, bucket, entry['s3_key_name'], file_stream)
                entry['md5_sum'] = assoc_file_stats['md5_sum']
                entry['file_size'] = assoc_file_stats['bytes_transferred']
            else:
                print "Key %s already found, skipping upload" % file_key.name
                # md5 the file
                m = md5.new()
                assoc_file_size = 0
                bytes_requested = 10485760
                chunk_read = file_key.read(size=bytes_requested)
                while len(chunk_read):
                    assoc_file_size += len(chunk_read)
                    m.update(chunk_read)
                    chunk_read = file_key.read(size=bytes_requested)

                entry['md5_sum'] = m.hexdigest()
                if assoc_file_size == file_key.size:
                    entry['file_size'] = assoc_file_size
                    print "md5 sum:", entry['md5_sum'], "size:", entry['file_size']
                else:
                    print "File size mismatch for %s, key reports %d, got %d" % (
                        file_key.name, file_key.size, assoc_file_size
                    )
                    sys.exit()

        if create_nodes:
            # create the nodes in psqlgraph
            with pq.session_scope() as session:

                # find aliquot ids
                for sub_id in aliquot_submitter_ids:
                    match = pq.nodes(mod.Aliquot).props(submitter_id=sub_id).one()
                    aliquot_ids.append(match.node_id)
                
                # create the file node for the tarball
                tarball_node_id, tarball_file_node = self.create_tarball_file_node(
                    pq, signpost, 
                    tarball_name, tarball_md5_sum, tarball_size, 
                    s3_key_name, project, 
                    node_data['participant_barcode']
                ) 

                # link the tarball file to the other nodes
                self.create_edges(pq, tarball_node_id, tarball_file_node, project)

                # create related files
                for entry in download_list:
                    self.create_related_file_node(
                        pq, signpost, 
                        entry, project, 
                        node_data['participant_barcode'],
                        tarball_file_node
                )
            
                # merge in our work
                pq.current_session().merge(tarball_file_node)
            #sys.exit()


    def process_all_work(self, ask_for_nih_creds=False, re_md5_tarball=False):
        print "process_all_work called with re_md5_tarball=", re_md5_tarball

        latest_log = self.find_latest_log("/home/ubuntu/logs")
        self.log_data, self.logged_keys = self.get_log_file_data(latest_log)

        if ask_for_nih_creds:
            auth_data = self.get_idpw()
        else:
            auth_data = self.dcc_creds

        # get top level list of folders
        for url1 in self.urls_to_check:
            project = url1.split("/")[-1]
            if len(project) < 2:
                project = url1.split("/")[-2]
            print "Getting url: %s for project %s" % (url1, project)
            dir_list = self.get_directory_list(url1, auth_data)
            print "%d tarballs to create" % len(dir_list)
            for entry in dir_list:
                print entry

            # iterate the list, creating the work
            for directory in dir_list:
                print directory
                self.process_job(directory, self.target_object_store, self.target_bucket_name, auth_data, project, re_md5_tarball)

if __name__ == '__main__':
    tdc_dl = TargetDCCCGIDownloader()
    tdc_dl.process_all_work(ask_for_nih_creds=True)

#main()
