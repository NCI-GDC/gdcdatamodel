from consul_manager import ConsulManager
import random
from signpostclient import SignpostClient
from gdcdatamodel.models import File
from psqlgraph import PsqlGraphDriver
from sqlalchemy import desc
from sqlalchemy.types import BIGINT
from cdisutils.log import get_logger
import os
import time
from urlparse import urlparse
import boto
import boto.s3.connection
from sqlalchemy import or_, not_
import requests
import json
from ds3client import client
from multiprocessing import Pool
try:
    requests.packages.urllib3.disable_warnings()
except:
    pass


def upload_file_wrapper(args):
    '''
    File upload wrapper for multiprocessing
    '''
    return upload_file(*args)


def upload_file(path, signpost, ds3_bucket, job, ds3_key, offset, length):
    '''
    Upload a single file within current job
    '''
    consul = ConsulManager(prefix='databackup')
    graph = PsqlGraphDriver(
        consul.consul_get(['pg', 'host']),
        consul.consul_get(['pg', 'user']),
        consul.consul_get(['pg', 'pass']),
        consul.consul_get(['pg', 'name'])
    )

    logger = get_logger('data_backup_{}_{}'.format(ds3_key.name, offset))
    logger.info('Start upload file %s with offset %s and length %s',
                ds3_key.name, offset, length)
    with graph.session_scope():
        current_file = graph.nodes().ids(ds3_key.name).one()

    urls = signpost.get(current_file.node_id).urls
    if not urls:
        logger.error(
            "no urls in signpost for file {}".format(current_file.file_name))
        return False
    parsed_url = urlparse(urls[0])
    (bucket_name, object_name) =\
        parsed_url.path.split("/", 2)[1:]
    s3 = boto.connect_s3(
        host=parsed_url.netloc,
        aws_access_key_id=consul.consul_get(['s3', parsed_url.netloc, 'access_key']),
        aws_secret_access_key=consul.consul_get(['s3', parsed_url.netloc, 'secret_key']),
        is_secure=False,
        calling_format=boto.s3.connection.OrdinaryCallingFormat()
    )

    logger.info("Get bucket from %s, %s, %s",
                s3.host, parsed_url.netloc, bucket_name)
    s3_bucket = s3.get_bucket(bucket_name)
    s3_key = s3_bucket.get_key(object_name)
    tmp_file = '/{}/{}_{}'.format(path, current_file.node_id, offset)
    logger.info("Download file %s offset %s length %s",
                current_file.file_name, offset, length)
    s3_key.get_contents_to_filename(
        tmp_file,
        headers={'Range': 'bytes={}-{}'.format(offset, offset+length-1)})
    logger.info('Upload file %s size %s from %s to %s bucket',
                current_file.file_name,
                current_file.file_size,
                parsed_url.netloc, ds3_bucket)
    with open(tmp_file, 'r') as f:
        ds3_key.put(f, job=job, offset=offset)
    logger.info('File %s uploaded' % current_file.file_name)
    os.remove(tmp_file)


class DataBackup(object):
    BACKUP_ACCEPT_STATE = ['backing_up', 'backuped', 'verified']
    BACKUP_FAIL_STATE = 'failed'
    BACKUP_DRIVER = ['primary_backup', 'storage_backup']
    CHUNK_SIZE = 107374182400
    BUCKET = 'gdc_backup'
    PROCESSES = 5

    def __init__(self, file_id='', bucket_prefix='', debug=False,
                 reportfile=None, driver='', constant=False):
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
            self.driver =\
                self.BACKUP_DRIVER[
                    random.randint(0, len(self.BACKUP_DRIVER)-1)]

        self.reportfile = reportfile
        if self.reportfile:
            self.report = {'start': time.time()}
        self.ds3 = client.Client(
            host=self.consul.consul_get(['ds3', self.driver, 'host']),
            port=self.consul.consul_get(['ds3', self.driver, 'port']),
            access_key=self.consul.consul_get(
                ['ds3', self.driver, 'access_key']),
            secret_key=self.consul.consul_get(
                ['ds3', self.driver, 'secret_key']),
            verify=False,
        )

        self.signpost = SignpostClient(self.consul.consul_get('signpost_url'))
        self.logger = get_logger('data_backup_{}'.format(str(os.getpid())))
        self.debug = debug
        self.path = self.consul.consul_get('path')
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
        self.job = self.ds3.jobs.put(
            self.BUCKET, *file_list, max_upload_size=104857600)

    def upload_chunks(self):
        '''
        Upload files in chunk
        '''
        ds3_bucket = self.get_bucket(self.BUCKET)
        uploaded = set()
        while(not self.job.is_completed):
            chunks = list(self.job.chunks(num_of_chunks=100))
            self.logger.info("Number of chunks: %s", len(chunks))
            for chunk in chunks:
                pool = Pool(processes=10)
                args = []
                chunksize = 0
                for arg in chunk:
                    chunksize += arg[-1]
                    cur_key = arg[0].name+'_'+str(arg[1])
                    if cur_key not in uploaded:
                        args.append(
                            (self.path, self.signpost, ds3_bucket, self.job)
                            + arg)
                    uploaded.add(cur_key)
                self.logger.info("Chunk size %s", chunksize)
                pool.map_async(upload_file_wrapper, args).get()
                pool.close()
                pool.join()

        self._record_file_state('backuped')

    @property
    def key(self):
        return self.file.node_id

    def get_s3(self, host):
        self.s3 = boto.connect_s3(
            host=host,
            aws_access_key_id=self.consul.consul_get(
                ['s3', host, 'access_key']),
            aws_secret_access_key=self.consul.consul_get(
                ['s3', host, 'secret_key']),
            is_secure=False,
            calling_format=boto.s3.connection.OrdinaryCallingFormat()
        )
        return self.s3

    def backup(self):
        self.get_files()
        if self.files:
            self.create_job()
            self.upload_chunks()

    def get_bucket(self, name):
        '''
        Get a bucket given a blackpearl driver name,
        if the bucket does not exist, create a new one
        '''
        results = list(self.ds3.buckets(name))
        if len(results) == 0:
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
        self.get_bucket(self.BUCKET)

        with self.consul.consul_session_scope(delay='1s'):
            while not self.consul.get_consul_lock('get_files'):
                time.sleep(1)
                self.logger.info("Can't get consul lock for get files")
            with self.graph.session_scope():
                conditions = []
                # Find all files who are not backuped or failed to
                # backup for either of the blackpearl
                conditions.append(not_(File._sysan.has_key(self.driver)))
                conditions.append((File._sysan[self.driver].astext
                                  == self.BACKUP_FAIL_STATE))
                query = self.graph.nodes(File).props({'state': 'live'})\
                    .filter(or_(*conditions))\
                    .order_by(desc(File.file_size.cast(BIGINT)))
                if query.count() == 0:
                    self.logger.info("We backed up all files")
                else:
                    nodes = query.yield_per(1000)
                    total_size = 0
                    for node in nodes:
                        # delete the file if it's already in blackpearl
                        if len(list(self.ds3.keys(
                                name=node.node_id, bucket_id=self.BUCKET))) != 0:
                            self.ds3.keys.delete(self.BUCKET, node.node_id)
                        self.files.append(node)
                        total_size += node.file_size
                        self.logger.info('Current size: %s' % total_size)
                        if total_size > self.CHUNK_SIZE:
                            self.logger.info('Selected %s size of %s files',
                                             total_size, len(self.files))

                            break
            if self.files:
                self._record_file_state('backing_up')

    def _record_file_state(self, state):
        with self.graph.session_scope() as session:
            for node in self.files:
                if not state:
                    if self.driver in node.system_annotations:
                        del node.system_annotations[self.driver]
                else:
                    node.system_annotations[self.driver] = state
                session.merge(node)
                session.commit()
