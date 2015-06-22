from consul_manager import ConsulManager
import datetime
import random
from signpostclient import SignpostClient
from gdcdatamodel.models import File
from psqlgraph import PsqlGraphDriver
from sqlalchemy import desc
from dateutil import parser
from sqlalchemy.types import BIGINT
from cdisutils.log import get_logger
from cdisutils.net import BotoManager
import os
import glob
import time
import math
import json
from urlparse import urlparse
import boto
import boto.s3.connection
from sqlalchemy import or_, not_
import requests
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
    logger = get_logger('data_backup_{}_{}'.format(ds3_key.name[0:8], offset))
    try:
        consul = ConsulManager(prefix='databackup')

        logger.info('Start upload file %s with offset %s and length %s',
                    ds3_key.name, offset, length)

        urls = signpost.get(ds3_key.name).urls
        if not urls:
            logger.error(
                "no urls in signpost for file {}".format(ds3_key.name))
            return False
        parsed_url = urlparse(urls[0])
        (bucket_name, object_name) =\
            parsed_url.path.split("/", 2)[1:]
        host = parsed_url.netloc
        download_file(ds3_key, offset, length,
                      bucket_name, object_name, path,
                      host,
                      consul.consul_get(['s3', host, 'port']),
                      consul.consul_get(['s3', host, 'access_key']),
                      consul.consul_get(['s3', host, 'secret_key']),
                      consul, logger)
        logger.info('Upload file %s from %s to %s bucket',
                    ds3_key.name,
                    parsed_url.netloc, ds3_bucket)

        tmp_file = '{}/{}_{}'.format(path, ds3_key.name, offset)
        with open(tmp_file, 'r') as f:
            ds3_key.put(f, job=job, offset=offset)
        logger.info('Part of file %s uploaded for job %s',
                    ds3_key.name, job.id)
        os.remove(tmp_file)

    except:
        logger.exception("Upload failed for file %s offset %s" %
                         (ds3_key.name, offset))
        raise


def download_file(ds3_key, offset, length, bucket_name,
                  object_name, path, host, port, access_key, secret_key,
                  consul, logger):
    logger.info("start download file %s", host)
    retries = 1
    while retries < 5:
        try:
            s3 = boto.connect_s3(
                host=host,
                port=port,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                is_secure=False,
                calling_format=boto.s3.connection.OrdinaryCallingFormat()
            )

            logger.info("Get bucket from %s, %s, %s",
                        s3.host, host, bucket_name)
            s3_bucket = s3.get_bucket(bucket_name)
            s3_key = s3_bucket.get_key(object_name)
            tmp_file = '{}/{}_{}'.format(path, ds3_key.name, offset)
            logger.info("Download file %s offset %s length %s",
                        ds3_key.name, offset, length)
            s3_key.get_contents_to_filename(
                tmp_file,
                headers={'Range': 'bytes={}-{}'.format(offset, offset+length-1)})
            return tmp_file
        except:
            logger.exception("ranged download failed for the %d times" % retries)
            retries += 1
            try: 
                os.remove(tmp_file)
            except:
                pass



def get_statistics(bucket='gdc_backup',
                   driver='primary_backup',
                   summary=False, output=''):
    consul = ConsulManager(prefix='databackup')
    graph = PsqlGraphDriver(
        consul.consul_get(['pg', 'host']),
        consul.consul_get(['pg', 'user']),
        consul.consul_get(['pg', 'pass']),
        consul.consul_get(['pg', 'name'])
    )
    ds3 = client.Client(
        host=consul.consul_get(['ds3', driver, 'host']),
        port=consul.consul_get(['ds3', driver, 'port']),
        access_key=consul.consul_get(
            ['ds3', driver, 'access_key']),
        secret_key=consul.consul_get(
            ['ds3', driver, 'secret_key']),
        verify=False,
    )
    keys = [[parser.parse(k.creation_date), k.name]
            for k in ds3.keys(bucket_id=bucket) if k.creation_date is not None]

    with graph.session_scope():
        for item in keys:
            item.append(graph.nodes().ids(item[1]).first().file_size)

    keys.sort()
    keys.reverse()
    total = sum(item[2] for item in keys)
    print "total size %s" % convert_size(total)
    end = datetime.datetime.now()
    start = end-datetime.timedelta(days=1)
    start_date = str(start.date())
    today = start_date
    result = {start_date: 0}
    for item in keys:
        while not (item[0] >= start and item[0] <= end):
            end = start
            start = end-datetime.timedelta(days=1)
            start_date = str(start.date())
            result[start_date] = 0
        result[start_date] += item[2]
    for key, value in result.iteritems():
        result[key] = convert_size(value)

    if summary:
        if output:
            with open(output, 'w') as f:
                json.dump(result, f)
        else:
            print result
    else:
        print 'Backed up last day: %s' % result[today]
    return result


def convert_size(size):
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size, 1024)))
    p = math.pow(1024, i)
    s = round(size/p, 2)
    if (s > 0):
        return '%s %s' % (s, size_name[i])
    else:
        return '0B'


class DataBackup(object):
    BACKUP_ACCEPT_STATE = ['backing_up', 'backuped', 'verified']
    BACKUP_FAIL_STATE = 'failed'
    BACKUP_DRIVER = ['primary_backup', 'storage_backup']
    CHUNK_SIZE = 107374182400
    BUCKET = 'gdc_backup'

    def __init__(self, debug=False, driver='', protocol='https'):
        self.consul = ConsulManager(prefix='databackup')
        self.upload_size = self.consul.consul_get('upload_size', 104857600)
        self.processes = self.consul.consul_get('processes', 5)
        self.graph = PsqlGraphDriver(
            self.consul.consul_get(['pg', 'host']),
            self.consul.consul_get(['pg', 'user']),
            self.consul.consul_get(['pg', 'pass']),
            self.consul.consul_get(['pg', 'name'])
        )
        s3_creds = {}
        for host in self.consul.consul_get('s3-endpoints'):
            s3_creds[host] = {'aws_access_key_id': self.consul.consul_get(['s3', host, 'access_key']),
                              'aws_secret_access_key': self.consul.consul_get(['s3', host, 'secret_key']),
                              'is_secure': False,
                              'port': self.consul.consul_get(['s3', host, 'port']),
                              'calling_format': boto.s3.connection.OrdinaryCallingFormat()}
        self.s3 = BotoManager(s3_creds)
        if driver:
            self.driver = driver
        else:
            self.driver =\
                self.BACKUP_DRIVER[
                    random.randint(0, len(self.BACKUP_DRIVER)-1)]

        self.ds3 = client.Client(
            host=self.consul.consul_get(['ds3', self.driver, 'host']),
            port=self.consul.consul_get(['ds3', self.driver, 'port']),
            access_key=self.consul.consul_get(
                ['ds3', self.driver, 'access_key'], ''),
            secret_key=self.consul.consul_get(
                ['ds3', self.driver, 'secret_key'], ''),
            protocol=protocol,
            verify=False,
        )

        self.signpost = SignpostClient(self.consul.consul_get('signpost_url'))
        self.logger = get_logger('data_backup_{}'.format(str(os.getpid())))
        self.debug = debug
        self.path = self.consul.consul_get('path')
        if not debug:
            self.logger.level = 30

    def create_job(self):
        '''
        Create a job with a list of files that's going to be uploaded
        '''
        file_list = [(node.node_id, node.file_size) for node in self.files]
        self.job = self.ds3.jobs.put(
            self.BUCKET, *file_list, max_upload_size=self.upload_size)

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
                pool = Pool(processes=self.processes)
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

    def backup(self):
        if self.consul.consul_get('kill'):
            self.logger.info("Safe kill from consul")
            return
        self.get_files()
        if self.files:
            try:
                self.create_job()
                self.upload_chunks()

                self._record_file_state('backuped')
                self.logger.info('Job %s succeed for files %s',
                                 self.job, self.files)
            except:
                self._record_file_state('failed')
                self.logger.exception('Job %s failed for files %s',
                                      self.job, self.files)
                self.job.delete()
            finally:
                self.cleanup()

    def cleanup(self):
        for f in self.files:
            prefix = '{}/{}*'.format(self.path, f.node_id)
            for filename in glob.glob(prefix):
                try:
                    os.remove(filename)
                    self.logger.info('Removed file %s', filename)
                except:
                    pass

    def get_bucket(self, name):
        '''
        Get a bucket given a blackpearl driver name,
        if the bucket does not exist, create a new one
        '''
        results = list(self.ds3.buckets(name))
        if len(results) == 0:
            bucket = self.ds3.buckets.create(name)
            with self.ds3.put('/_rest_/bucket/{}/?default_write_optimization=PERFORMANCE'
                              .format(name)):
                self.logger.info("create bucket with performance mode")
            return bucket
        else:
            return results[0]

    def get_key_size(self, url):
        file_size = self.s3.get_url(url).size
        return file_size

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
                    .not_sysan({'md5_verify_status': 'failed'})\
                    .filter(or_(*conditions))\
                    .order_by(desc(File.file_size.cast(BIGINT)))
                if query.count() == 0:
                    self.logger.info("We backed up all files")
                else:
                    nodes = query.yield_per(1000)
                    total_size = 0
                    for node in nodes:
                        s3_key_size = self.get_key_size(self.signpost.get(node.node_id).urls[0])
                        if s3_key_size != node.file_size:
                            self._record_file_state('error', node)
                            self.logger.info("file %s is corrupted, graph reported %d, s3 reported %d" 
                                             % (node.file_name, s3_key_size, node.file_size))
                            continue
                        # delete the file if it's already in blackpearl
                        self.logger.info('check file')
                        if len(list(self.ds3.keys(
                                name=node.node_id,
                                bucket_id=self.BUCKET))) != 0:
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

    def _record_file_state(self, state, node=None):
        self.logger.info("Mark files %s as state %s", self.files, state)
        with self.graph.session_scope() as session:
            if node is None:
                for node in self.files:
                    node.system_annotations[self.driver] = state
                    session.merge(node)
            else:
                node.system_annotations[self.driver] = state
                session.merge(node)
