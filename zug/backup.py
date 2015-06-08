from consul_manager import ConsulManager
import random
from signpostclient import SignpostClient
from gdcdatamodel.models import File
from psqlgraph import PsqlGraphDriver
from sqlalchemy import desc
from sqlalchemy.types import BIGINT
from cdisutils.log import get_logger
import os
from base64 import b64encode
import time
from binascii import unhexlify
from urlparse import urlparse
import boto
import boto.s3.connection
from sqlalchemy import or_, not_
from boto.s3.key import Key, KeyFile
import requests
import json
from ds3client import client
from multiprocessing import Pool
try:
    requests.packages.urllib3.disable_warnings()
except:
    pass


# Monkey patch boto key close method, so when streaming from s3 to blackpearl,
# timeout caused by blackpearl won't read the whole key
def close(cls, fast=False):
    cls.closed = True
    cls.resp = None
    cls.mode = None

Key.close = close

class RangeKeyFile(KeyFile):
    def __init__(self, key, offset, length):
        super(RangeKeyFile, self).__init__(key)
        self.length = length
        self.offset = offset
        self.seek(offset)

class DataBackup(object):
    BACKUP_ACCEPT_STATE = ['backing_up', 'backuped', 'verified']
    BACKUP_FAIL_STATE = 'failed'
    BACKUP_DRIVER = ['primary_backup', 'storage_backup']
    CHUNK_SIZE = 107374182400
    BUCKET = 'gdc_backup'
    PROCESSES = 5

    def __init__(self, file_id='', bucket_prefix='', debug=False,
                 reportfile=None, driver = '', constant=False):
        self.consul = ConsulManager(prefix='databackup')
        self.graph = PsqlGraphDriver(
            self.consul.consul_get(['pg', 'host']),
            self.consul.consul_get(['pg', 'user']),
            self.consul.consul_get(['pg', 'pass']),
            self.consul.consul_get(['pg', 'name'])
        )
        if driver:
            self.driver = driver
        else:
            self.driver = self.BACKUP_DRIVER[random.randint(0, len(self.BACKUP_DRIVER)-1)]

        self.reportfile = reportfile
        if self.reportfile:
            self.report = {'start': time.time()}
        self.ds3 = client.Client(
            host=self.consul.consul_get(['ds3', self.driver, 'host']),
            port=self.consul.consul_get(['ds3', self.driver, 'port']),
            access_key=self.consul.consul_get(['ds3', self.driver, 'access_key']),
            secret_key=self.consul.consul_get(['ds3', self.driver, 'secret_key']),
            verify=False,
        )

        self.signpost = SignpostClient(self.consul.consul_get('signpost_url'))
        self.logger = get_logger('data_backup_{}'.format(str(os.getpid())))
        self.debug = debug
        if not debug:
            self.logger.level = 30
        self.file_id = file_id
        self._bucket_prefix = bucket_prefix
        self.constant = constant



    def create_job(self):
        '''
        Create a job with a list of files that's going to be uploaded
        '''
        file_list = [(node.node_id, node.file_size) for node in self.files]
        self.job = self.ds3.jobs.put(self.BUCKET, *file_list)

    def upload_chunks(self):
        '''
        Upload files in chunk
        '''
        while(not self.job.is_completed):
            for chunk in self.job.chunks(num_of_chunks=100):

                for args in chunk:
                    self.upload_file(*args)


        self._record_file_state('backuped')

        
    def upload_file(self, ds3_key, offset, length):
        '''
        Upload a single file within current job
        '''
        self.logger.info('Start upload file %s with offset %s and length %s', ds3_key.name, offset,length)
        with self.graph.session_scope():
            current_file =  self.graph.nodes().ids(ds3_key.name).one()

        urls = self.signpost.get(current_file.node_id).urls
        if not urls:
            self.logger.error(
                "no urls in signpost for file {}".format(current_file.file_name))
            return False
        parsed_url = urlparse(urls[0])
        if parsed_url.netloc == 'ceph.service.consul':
            netloc = 'kh10-9.osdc.io'
        if parsed_url.netloc == 'cleversafe.service.consul':
            netloc = 'gdc-accessor2.osdc.io'
       
        self.get_s3(netloc)
        (self.bucket_name, self.object_name) =\
            parsed_url.path.split("/", 2)[1:]
        self.logger.info("Get bucket from %s, %s, %s", self.s3.host, parsed_url.netloc, self.bucket_name)
        s3_bucket = self.s3.get_bucket(self.bucket_name)
        s3_key = s3_bucket.get_key(self.object_name)
        tmp_file = '/home/ubuntu/{}_{}'.format(current_file.node_id, offset)
        s3_key.get_contents_to_filename(tmp_file,
            headers={'Range': 'bytes={}-{}'.format(offset, length)})
        ds3_bucket = self.get_bucket(self.BUCKET)
        self.logger.info('Upload file %s size %s from %s to %s bucket %s',
                         current_file.file_name, current_file.file_size, parsed_url.netloc, self.driver, self.BUCKET)
        
        if self.reportfile:
            self.report[self.driver+'_start'] = time.time()
        with open(tmp_file, 'r') as f:
            ds3_key.put(f, job=self.job, offset=offset)
        self.logger.info('File %s uploaded' % current_file.file_name)
        if self.reportfile:
            self.report[self.driver+'_end'] = time.time()


    @property
    def key(self):
        return self.file.node_id

    def get_s3(self, host):
        self.s3 = boto.connect_s3(
            host=host,
            aws_access_key_id=self.consul.consul_get(['s3', host, 'access_key']),
            aws_secret_access_key=self.consul.consul_get(['s3', host, 'secret_key']),
            is_secure=False,
            calling_format=boto.s3.connection.OrdinaryCallingFormat()
        )

    def backup(self):
        self.get_files()
        if self.files:
            self._record_file_state('backing_up')
            self.create_job()
            self.upload_chunks()

    def upload(self):
        urls = self.signpost.get(self.file.node_id).urls
        if not urls:
            self.logger.error(
                "no urls in signpost for file {}".format(self.file.file_name))
            return False
        parsed_url = urlparse(urls[0])
        self.get_s3(parsed_url.netloc)
        (self.bucket_name, self.object_name) =\
            parsed_url.path.split("/", 2)[1:]
        self.logger.info("Get bucket from %s, %s, %s", self.s3.host, parsed_url.netloc, self.bucket_name)
        s3_bucket = self.s3.get_bucket(self.bucket_name)
        s3_key = s3_bucket.get_key(self.object_name)
        keyfile = KeyFile(s3_key)
        ds3_bucket = self.get_bucket(self.driver, self.BUCKET)
        key = Key(ds3_bucket)
        key.key = s3_key.key
        self.logger.info('Upload file %s size %s from %s to %s bucket %s',
                         self.file.file_name, self.file.file_size, parsed_url.netloc, self.driver, self.BUCKET)
        
        if self.reportfile:
            self.report[self.driver+'_start'] = time.time()

        if ds3_bucket.get_key(key.key):
            self.logger.info("File already exists, delete old one")
            ds3_bucket.delete_key(key.key)
        key.set_contents_from_file(keyfile,
                md5=(self.file.md5sum, b64encode(unhexlify(self.file.md5sum))))
        self.logger.info('File %s uploaded' % self.file.file_name)
        if self.reportfile:
            self.report[self.driver+'_end'] = time.time()
        with self.graph.session_scope() as session:
            self.file.system_annotations[self.driver] = 'backuped'
            session.merge(self.file)
            session.commit()

    def key_exists(self, bucket, key):
        if bucket.get_key(key):
            return True
        return False

    def get_bucket(self, name):
        '''
        Get a bucket given a blackpearl driver name,
        if the bucket does not exist, create a new one
        '''
        results = list(self.ds3.buckets(name))
        if len(results) == 0 :
            return self.ds3.buckets.create(name)
        else:
            return results[0]


    def cleanup(self):
        if not self.constant:
            super(DataBackup, self).cleanup()
        succeed = False
        if self.reportfile:
            succeed = False
            for driver in self.BACKUP_DRIVER:
                if driver+'_end' in self.report:
                    succeed = True
            if succeed:
                self.report['end'] = time.time()
                self.report['node_id'] = self.file.node_id
                self.report['size'] = self.file.file_size
                with open(self.reportfile, 'a') as f:
                    f.write(json.dumps(self.report))
                    f.write('\n')
        self.file_id = ''
        self.file = None

    def get_files(self):
        '''
        get a list of files that's not backuped and totals up to
        100gb
        '''
        self.files = []
        ds3_bucket = self.get_bucket(self.BUCKET)
        with self.graph.session_scope():
            conditions = []
            # Find all files who are not backuped or failed to 
            # backup for either of the blackpearl
            for driver in self.BACKUP_DRIVER:
                conditions.append(not_(File._sysan.has_key(driver)))
                conditions.append((File._sysan[driver].astext
                                  == self.BACKUP_FAIL_STATE))
            query = self.graph.nodes(File).props({'state': 'live'})\
                .filter(or_(*conditions))
            if query.count() == 0:
                self.logger.info("We backed up all files")
            else:
                nodes = query.yield_per(1000)
                print 'test'
                total_size = 0
                for node in nodes:
                    # delete the file if it's already in blackpearl
                    if len(list(self.ds3.keys(name=node.node_id, bucket_id=self.BUCKET))) != 0:
                        self.ds3.keys.delete(self.BUCKET, node.node_id)
                    self.files.append(node)
                    total_size += node.file_size
                    self.logger.info('Current size: %s' % total_size)
                    if total_size > self.CHUNK_SIZE:
                        self.logger.info('Selected %s size of %s files',
                                total_size, len(self.files))
                        break

    def _record_file_state(self,state):
        with self.graph.session_scope() as session:
            for node in self.files:
                node.system_annotations[self.driver] = state 
                session.merge(node)

    def get_file_to_backup(self):
        '''
        If a file_id is specified at the beginning, it will try to
        back up that file, otherwise select a random file that's
        not backed up in psqlgraph
        '''
        if not self.file_id:
            with self.graph.session_scope():
                conditions = []
                # Find all files who are not backuped or failed to 
                # backup for either of the blackpearl
                conditions.append(not_(File._sysan.has_key(self.driver)))
                conditions.append((File._sysan[self.driver].astext
                                  == self.BACKUP_FAIL_STATE))
                query = self.graph.nodes(File).props({'state': 'live'})\
                    .filter(or_(*conditions))\
                    .order_by(desc(File.file_size.cast(BIGINT)))\
                    .limit(10000)
                if query.count() == 0:
                    self.logger.info("We backed up all files")
                else:
                    if self.constant:
                        self.file = query.first()
                        self.file_id = self.file.node_id
                        return self.file
                    tries = 5
                    while tries > 0:
                        tries -= 1
                        max_count = query.count()
                        self.file = query[
                            random.randint(0, max_count-1)]
                        if self.constant or self.get_consul_lock(self.file_id):
                            self.file_id = self.file.node_id
                            return self.file
                    self.logger.error(
                        "Can't acquire lock after 5 tries")
        else:
            with self.graph.session_scope():
                self.file = self.graph.nodes(File).ids(self.file_id).one()
                self.logger.info(self.file.system_annotations)
                valid = False
                if (self.driver not in self.file.system_annotations) or\
                        (self.file.system_annotations[self.driver] ==
                            self.BACKUP_FAIL_STATE):
                    valid = True
                if not valid:
                    self.logger.error("File {} already backed up"
                                      .format(self.file.file_name))
                    return
                if self.constant:
                    return self.file
                if not self.get_consul_lock(self.file_id):
                    self.logger.error(
                        "Can't acquire lock for file {}".
                        format(self.file.file_name))
                    return
                return self.file
