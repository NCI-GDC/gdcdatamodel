#!/usr/bin/env python

import os,sys
import requests
from requests.auth import HTTPBasicAuth
import base64
import getpass
from bs4 import BeautifulSoup
import urllib3
import logging
from cdisutils.log import get_logger
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

class TargetDCCCGIDownloader(object):
    """Main class to handle TARGET DCC CGI download"""
    def __init__(self, signpost_client=None):

        self.tag_names = [
            "PilotAnalysisPipeline2/",
            "OptionAnalysisPipeline2/"
        ]

        self.projects = [
            "AML",
            "NBL",
        #    "WT",
        ]

        # ignore the header line
        self.strings_to_ignore = [ 
            "Name", "Last modified", 
            "Size", "Parent Directory", 
            "lost+found/", "Parent Directory" ]

        self.url_base = "https://target-data.nci.nih.gov/"
        self.url_path = "/Discovery/WGS/CGI/"

        self.urls_to_check = []
        for project in self.projects:
            for tag in self.tag_names:
                self.urls_to_check.append("%s%s%s%s" % (
                    self.url_base, project,
                    self.url_path, tag
                ))

        # right now, these are the projects to check
        #self.urls_to_check = [
        #    "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/PilotAnalysisPipeline2/",
        #    "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/" ]

        self.log = get_logger("target_dcc_cgi_project_sync_" + str(os.getpid()))

        # kinda hacky, but the table for the file browse uses these classes when we know
        # we're looking at file/directory lines
        self.row_classes = [ "even", "odd" ]
        self.count = 0
        self.total_size = 0

        if not signpost_client:
            if 'SIGNPOST_URL' in os.environ:
                self.signpost_url = os.environ['SIGNPOST_URL']
            else:
                self.signpost_url = "http://signpost.service.consul"
            self.signpost = SignpostClient(self.signpost_url)
        else:
            self.signpost = signpost_client
        cur_time = datetime.datetime.now()
        self.checkpoint_dir = "/home/ubuntu/checkpoint"
        self.checkpoint_filename = "%s/added_target_dcc_cgi_files_%04d-%02d-%02d_%02d-%02d-%02d.json" % (
            self.checkpoint_dir,
            cur_time.year, cur_time.month, cur_time.day,
            cur_time.hour, cur_time.now().minute, cur_time.second)
        self.pq_creds = {}
        self.dcc_creds = {}
        self.checkpoint_data = []
        self.processed_keys = []

        # edge data
        self.platform = "Complete Genomics"
        self.data_subtype = "CGI Archive"
        self.data_format = "TARGZ"
        self.experimental_strategy = "WGS"

        # trying to make it look more smartly because the env names seem
        # to change so much depending (QA_PG_HOST vs. ZUGS_PG_HOST, etc)
        for env in os.environ.keys():
            if env.find('PG_HOST') != -1:
                self.pq_creds['host_name'] = os.environ[env]
            if env.find('PG_USER') != -1:
                self.pq_creds['user_name'] = os.environ[env]
            if env.find('PG_NAME') != -1:
                self.pq_creds['db_name'] = os.environ[env]
            if env.find('PG_PASS') != -1:
                self.pq_creds['password'] = os.environ[env]
            if env.find('DCC_USER') != -1:
                self.dcc_creds['id'] = os.environ[env]
            if env.find('DCC_PASS') != -1:
                self.dcc_creds['pw'] = os.environ[env]

        #self.target_acls = ["phs000471", "phs000218"]
        self.target_acls = []

        if 'S3_HOST' in os.environ:
            self.target_object_store = os.environ['S3_HOST'].split('.')[0]

        if 'TARGET_PROTECTED_BUCKET' in os.environ:
            self.target_bucket_name = os.environ['TARGET_PROTECTED_BUCKET']
        else:
            raise RuntimeError("Warning, TARGET_PROTECTED_BUCKET not found")


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
        return value

    def write_json_to_file(self, data):
        with open(self.checkpoint_filename, "a") as checkpoint_file:
            json.dump(data, checkpoint_file)
            checkpoint_file.write("\n")

    def process_tree(self, url, url_list, test_download=False):
        """Walk the given url and recursively find all the file links."""
        r = requests.get(url, auth=(self.dcc_creds['id'], self.dcc_creds['pw']), verify=False)
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
                        self.process_tree(url + dir_name, url_list)
                    # file
                    else:
                        if image['alt'].find("DIR") == -1:
                            file_name = row.find('td', class_="indexcolname").get_text().strip()
                            link = row.find('a')
                            file_url = url + link['href']
                            if test_download == False:
                                url_list.append(file_url)
                            else:
                                self.log.info("Downloading file: %s to %s from %s" % (file_name, os.getcwd(), file_url))
                                open(file_name, 'a').close()

    def get_directory_list(self, url):
        """ Get top level list of directories to walk."""
        directory_list = []
        r = requests.get(url, auth=(self.dcc_creds['id'], self.dcc_creds['pw']), verify=False)
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
            error_str = "Unable to get url", url, r.status_code, r.reason
            self.log.error(error_str)
            raise RuntimeError(error_str)

        return directory_list

    def create_signpost_entry(self, node_id, uri):
        doc = self.signpost.get(node_id)
        doc.urls = [uri]
        doc.patch()

    def create_signpost_uri(self, object_store, bucket, key_name):
        return "s3://%s/%s/%s" % (object_store, bucket, key_name)

    def create_tarball_file_node(self, pq,  
        tarball_name, tarball_md5_sum, tarball_size, 
        s3_key_name, participant_barcode):
        """Create the file node in psqlgraph for the tarball."""

        tarball_node_id = "0"
        file_node = None
        node_exists = False

        # see if we exist before getting a new ID
        results = pq.nodes(mod.File).props(file_name=tarball_name).all()
        if len(results) > 0:
            if len(results) > 1:
                raise RuntimeError("More than one file found with that name")
            else:
                tarball_node_id = results[0].node_id
                file_node = results[0]
                node_exists = True

        if not node_exists:
            # check if file is in signpost yet
            tarball_node_id = self.signpost.create().did
            tarball_uri = self.create_signpost_uri(self.target_object_store, self.target_bucket_name, s3_key_name)
            self.create_signpost_entry(tarball_node_id, tarball_uri)
            file_properties = {
                'state': "submitted",
                'file_size': tarball_size,
                'md5sum': tarball_md5_sum,
                'file_name': tarball_name,
                'submitter_id': participant_barcode,
                'state_comment': None
            }

            file_sysan = {
                'source': 'target_dcc_cgi',
                '_participant_barcode': participant_barcode
            }

            # add the new node
            file_node = mod.File(
                node_id=tarball_node_id,
                acl=self.target_acls,
                properties=file_properties,
                system_annotations=file_sysan
            )
        else:
            # verify that the signpost ID works
            tarball_uri = self.create_signpost_uri(self.target_object_store, self.target_bucket_name, s3_key_name)
            # NB: we might not just be able to blow away at a point,
            # but for now, if it exists, just reset to our uri
            self.create_signpost_entry(tarball_node_id, tarball_uri)

            file_node.properties.update({
                'state': "submitted",
                'file_size': tarball_size,
                'md5sum': tarball_md5_sum,
                'file_name': tarball_name,
                'submitter_id': participant_barcode,
                'state_comment': None
            })

            file_node.system_annotations.update({
                'source': 'target_dcc_cgi',
                '_participant_barcode': participant_barcode
            })
            file_node.acl = self.target_acls

        return tarball_node_id, file_node

    def create_related_file_node(self, pq, entry, participant_barcode, tarball_file_node):
        """Create a node for a related file in psqlgraph."""
        if tarball_file_node != None:
            add_related_file = True
            related_file_node = None
            # check if the node already has an edge to a file with this name
            for related_file in tarball_file_node.related_files:
                if related_file.file_name == entry['file_name']:
                    add_related_file = False
                    related_file_node = related_file

            if add_related_file:

                # get an id from signpost
                assoc_file_node_id = self.signpost.create().did
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
                    'submitter_id': participant_barcode,
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

                tarball_file_node.related_files.append(file_node)

                self.create_signpost_entry(assoc_file_node_id, assoc_file_uri)
            else:
                # check signpost
                assoc_file_uri = self.create_signpost_uri(
                    self.target_object_store, 
                    self.target_bucket_name, 
                    entry['s3_key_name'])
                # NB: we might not just be able to blow away at a point,
                # but for now, if it exists, just reset to our uri
                self.create_signpost_entry(related_file_node.node_id, assoc_file_uri)

                # reset data
                related_file_node.properties.update({
                    'state': "submitted",
                    'md5sum': entry['md5_sum'],
                    'file_size': entry['file_size'],
                    'file_name': entry['file_name'],
                    'submitter_id': participant_barcode,
                    'state_comment': None
                })

                related_file_node.system_annotations.update({
                    'source': 'target_dcc_cgi',
                    '_participant_barcode': participant_barcode
                })
                related_file_node.acl = self.target_acls

    def create_edges(self, pq, tarball_node_id, tarball_file_node, project, tag):
        """Create the edges to a given tarball node in psqlgraph."""
        if tarball_file_node is not None:
            # platform
            if len(tarball_file_node.platforms) == 0:
                platform_node = pq.nodes(mod.Platform).props(name=self.platform).one()
                tarball_file_node.platforms.append(platform_node)

            # data_subtype
            if len(tarball_file_node.data_subtypes) == 0:
                data_subtype_node = pq.nodes(mod.DataSubtype).props(name=self.data_subtype).one()
                tarball_file_node.data_subtypes.append(data_subtype_node)

            # data_format
            if len(tarball_file_node.data_formats) == 0:
                data_format_node = pq.nodes(mod.DataFormat).props(name="TARGZ").one()
                tarball_file_node.data_formats.append(data_format_node)

            # experimental_strategy
            if len(tarball_file_node.experimental_strategies) == 0:
                experimental_strategy_node = pq.nodes(mod.ExperimentalStrategy).props(name=self.experimental_strategy).one()
                tarball_file_node.experimental_strategies.append(experimental_strategy_node)

            # TODO: project when datamodel changes to support

            # tag
            if len(tarball_file_node.tags) == 0:
                tag_node = pq.nodes(mod.Tag).props(name=tag).one()
                tarball_file_node.tags.append(tag_node)

    def find_latest_checkpoint(self, directory):
        """Find the latest checkpoint file.
        
        This routine is here because right now we're
        storing the results of the file creation in a
        checkpoint file, either a) because something
        could go wrong and it's time intensive to 
        re-md5 a file, or b) it's a convenient way
        to re-run the node/edge creation separate from
        the downloading.

        TODO: Move this to consul key/values.
        """

        cur_dir = os.getcwd()
        os.chdir(directory)
        newest = max(os.listdir("."), key = os.path.getctime)
        os.chdir(cur_dir)
        return directory + "/" + newest

    def get_checkpoint_file_data(self, checkpoint_file_name):
        """Get the data from the checkpoint file.

        Generally, we expect to get the key names, md5 sums, and
        file sizes.
        """

        file_data = []
        processed_files = []
        with open(checkpoint_file_name, "r") as checkpoint_file:
            for line in checkpoint_file:
                data = json.loads(line)
                if "file_name" in data:
                    file_data.append(data)
                    processed_files.append(data['s3_key_name'])
        return file_data, processed_files

    def process_job(self, directory, object_store, bucket, project, tag, re_md5_tarball=True):
        """The main routine to create a tarball and create nodes/edges given a starting point."""
        self.log.info("Processing %s for project %s with recheck=%u" % (
            directory['dir_name'], project, re_md5_tarball
        ))

        create_nodes = True 
        node_data = {}
        url_list = []
        tar_list = []
        download_list = []
        aliquot_submitter_ids = set()
        aliquot_ids = []
        pq = self.connect_to_psqlgraph() 
        link_count = 0



        self.write_json_to_file({
            'directory name':directory['dir_name'],
            'project':project,
            'tag':tag
        })

        tarball_name = "%s.%s.tar.gz" % (directory['dir_name'].strip('/'), tag)
        self.log.info("tarball: %s" % tarball_name)

        node_data['participant_barcode'] = directory['dir_name'].strip("/")

        # parse the tree to find files to archive/download
        self.process_tree(directory['url'], url_list)
        self.log.info("Tree complete: %s" % directory['url'])
        self.log.info("%d items in list" % len(url_list))

        # find files to exclude
        for entry in url_list:
            url_parts = entry.split("/")
            if url_parts[-2] == "EXP":
                dl_entry = {}
                dl_entry['url'] = entry
                dl_entry['s3_key_name'] = project + "/" + tag + "/" + url_parts[-3] + "/" + url_parts[-1]
#                dl_entry['s3_key_name'] = tag + "/" + url_parts[-3] + "/" + url_parts[-1]
                dl_entry['file_name'] = url_parts[-1]
                download_list.append(dl_entry)
            if url_parts[-3] == "EXP":
                aliquot_submitter_ids.add(url_parts[-2])
            tar_list.append(Stream(entry, entry, self.dcc_creds))
        
        self.log.info("%d items in tar list" % len(tar_list))
        self.log.info("%d items in download list" % len(download_list))
        for entry in download_list:
            self.log.info(entry['s3_key_name'])
        self.log.info("%d aliquot submitter ids found" % len(aliquot_submitter_ids))
        self.log.info(aliquot_submitter_ids)
        
        # check if the key already exists
        s3_inst = S3_Wrapper()
        s3_conn = s3_inst.connect_to_s3(object_store)
        s3_key_name = "%s/%s/%s%s" % (project, tag, directory['dir_name'], tarball_name) 
#        s3_key_name = "%s/%s%s" % (tag, directory['dir_name'], tarball_name) 
        file_key = s3_inst.get_file_key(s3_conn, bucket, s3_key_name)

        # TODO: if the key does exist, we need to check if the archive needs to be updated
        if file_key == None:
            self.log.info("Key %s not found, downloading and creating archive" % s3_key_name)

            # create the tarball
            tar_ball = TarStream(tar_list, tarball_name)

            # upload tarball to s3
            s3_key_name = "%s/%s/%s%s" % (project, tag, directory['dir_name'], tarball_name) 
#            s3_key_name = "%s/%s%s" % (tag, directory['dir_name'], tarball_name) 
            upload_stats = s3_inst.upload_multipart_file(s3_conn, bucket, s3_key_name, tar_ball, True)
            tarball_md5_sum = upload_stats['md5_sum']
            tarball_size = upload_stats['bytes_transferred']
            self.log.info("md5 sum: %s size: %d" % (tarball_md5_sum, tarball_size))
            self.write_json_to_file({
                'file_name':tarball_name,
                's3_key_name':s3_key_name,
                'md5_sum': tarball_md5_sum,
                'file_size': tarball_size,
            })
        else:
            self.log.info("Key %s already found (%d), skipping upload" % (
                file_key.name, file_key.size))
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
                self.log.info("skipping md5 recheck")
                tarball_md5_sum = ""
                tarball_size = None
                for file_data in self.checkpoint_data:
                    if file_data['s3_key_name'] == file_key.name:
                        tarball_md5_sum = file_data['md5_sum']
                        tarball_size = file_data['file_size']
                        break
                if tarball_size:
                    self.log.info("Unable to find key %s in checkpoint file" % file_key.name)

            if tarball_size == file_key.size:
                self.log.info("md5 sum: %s size: %d" % (tarball_md5_sum, tarball_size))
                self.write_json_to_file({
                    'file_name':tarball_name,
                    's3_key_name':s3_key_name,
                    'md5_sum': tarball_md5_sum,
                    'file_size': tarball_size,
                })
            else:
                if file_key.size:
                    error_str = "File size mismatch for %s, key reports %d, got %d" % (
                        file_key.name, int(file_key.size), int(tarball_size)
                    )
                else:
                    error_str = "File size mismatch for %s, key not found, got %d" % (
                        file_key.name, int(tarball_size)
                    )
                    
                self.log.error(error_str)
                raise RuntimeError(error_str)

        # transfer the other files to the object store
        for entry in download_list:
            self.log.info(entry)
            # check if the key exists
            file_key = s3_inst.get_file_key(s3_conn, bucket, entry['s3_key_name']) 
            if file_key == None:
                self.log.info("File %s not present, uploading" % entry['s3_key_name'])
                file_stream = Stream(entry['url'], entry['url'].split("/")[-1], self.dcc_creds)
                file_stream.connect()
                assoc_file_stats = s3_inst.upload_multipart_file(s3_conn, bucket, entry['s3_key_name'], file_stream)
                entry['md5_sum'] = assoc_file_stats['md5_sum']
                entry['file_size'] = assoc_file_stats['bytes_transferred']
            else:
                self.log.info("Key %s already found, skipping upload" % file_key.name)
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
                    self.log.info("md5 sum: %s size: %d" % (entry['md5_sum'], entry['file_size']))
                else:
                    error_str = "File size mismatch for %s, key reports %d, got %d" % (
                        file_key.name, file_key.size, assoc_file_size
                    )
                    self.log.error(error_str)
                    raise RuntimeError(error_str)

        if create_nodes:
            # create the nodes in psqlgraph
            with pq.session_scope() as session:

                # find aliquot ids
                for sub_id in aliquot_submitter_ids:
                    match = pq.nodes(mod.Aliquot).props(submitter_id=sub_id).one()
                    aliquot_ids.append(match.node_id)
                
                # create the file node for the tarball
                tarball_node_id, tarball_file_node = self.create_tarball_file_node(
                    pq, tarball_name, tarball_md5_sum, tarball_size, 
                    s3_key_name, node_data['participant_barcode']
                ) 

                # link the tarball file to the other nodes
                self.create_edges(pq, tarball_node_id, tarball_file_node, project, tag)

                # create related files
                for entry in download_list:
                    self.create_related_file_node(
                        pq, entry, node_data['participant_barcode'],
                        tarball_file_node
                )
            
                # merge in our work
                pq.current_session().merge(tarball_file_node)


    def process_all_work(self, re_md5_tarball=False):
        """Outer loop to delegate work for each directory."""
        self.log.info("process_all_work called with re_md5_tarball = %s" % re_md5_tarball)

        latest_checkpoint = self.find_latest_checkpoint(self.checkpoint_dir)
        self.checkpoint_data, self.processed_keys = self.get_checkpoint_file_data(latest_checkpoint)

        # get top level list of folders
        for url1 in self.urls_to_check:
            project = filter(None, url1.split("/"))[2]
            tag = filter(None, url1.split("/"))[-1]
            self.log.info("Getting url: %s for project %s, tag %s" % (url1, project, tag))
            dir_list = self.get_directory_list(url1)
            self.log.info("%d tarballs to create" % len(dir_list))
            for entry in dir_list:
                self.log.info(entry)

            # iterate the list, creating the work
            for directory in dir_list:
                self.log.info("Processing: %s" % directory)
                self.process_job(directory, self.target_object_store, self.target_bucket_name, project, tag, re_md5_tarball)

if __name__ == '__main__':
   
    tdc_dl = TargetDCCCGIDownloader()
    if len(sys.argv) > 1:
        for arg in sys.argv:
            if arg.find("DCC_USER") != -1:
                tdc_dl.dcc_creds['id'] = arg.split('=')[1]
            if arg.find("DCC_PASS") != -1:
                tdc_dl.dcc_creds['pw'] = arg.split('=')[1]

    tdc_dl.process_all_work()
