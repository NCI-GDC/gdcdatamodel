from consul_mixin import ConsulMixin
import random
from signpostclient import SignpostClient
from gdcdatamodel.models import File
from psqlgraph import PsqlGraphDriver
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
from  sqlalchemy.sql.expression import func
import json

try:
    requests.packages.urllib3.disable_warnings()
except:
    pass


class DataBackup(ConsulMixin):
    BACKUP_ACCEPT_STATE = ['backuped', 'verified']
    BACKUP_FAIL_STATE = 'failed'
    BACKUP_DRIVER = ['primary_backup', 'storage_backup']

    def __init__(self, file_id='', bucket_prefix='', debug=False,
                 reportfile=None, driver = ''):
        super(DataBackup, self).__init__()
        self.graph = PsqlGraphDriver(
            self.consul_get(['pg', 'host']),
            self.consul_get(['pg', 'user']),
            self.consul_get(['pg', 'pass']),
            self.consul_get(['pg', 'name'])
        )
        self.reportfile = reportfile
        if self.reportfile:
            self.report = {'start': time.time()}
        self.ds3 = {}
        for driver in self.BACKUP_DRIVER:
            self.ds3[driver] = boto.connect_s3(
                host=self.consul_get(['ds3', driver, 'host']),
                port=self.consul_get(['ds3', driver, 'port']),
                aws_access_key_id=self.consul_get(['ds3', driver, 'access_key']),
                aws_secret_access_key=self.consul_get(['ds3', driver, 'secret_key']),
                is_secure=False,
                calling_format=boto.s3.connection.OrdinaryCallingFormat()
            )
        self.s3 = boto.connect_s3(
            host=self.consul_get(['s3', 'host']),
            aws_access_key_id=self.consul_get(['s3', 'access_key']),
            aws_secret_access_key=self.consul_get(['s3', 'secret_key']),
            is_secure=False,
            calling_format=boto.s3.connection.OrdinaryCallingFormat()
        )
        self.signpost = SignpostClient(self.consul_get('signpost_url'))
        self.logger = get_logger('data_backup_{}'.format(str(os.getpid())))
        self.debug = debug
        if not debug:
            self.logger.level = 30
        self.file_id = file_id
        self._bucket_prefix = bucket_prefix

    @property
    def key(self):
        return self.file.node_id

    def backup(self):
        with self.consul_session_scope():
            if not self.get_file_to_backup():
                return
            self.upload()

    def upload(self):
        urls = self.signpost.get(self.file.node_id).urls
        if not urls:
            self.logger.error(
                "no urls in signpost for file {}".format(self.file.file_name))
            return False
        parsed_url = urlparse(urls[0])
        (self.bucket_name, self.object_name) =\
            parsed_url.path.split("/", 2)[1:]
        s3_bucket = self.s3.get_bucket(self.bucket_name)
        if self._bucket_prefix:
            ds3_bucket_name = self._bucket_prefix+'_'+self.bucket_name
        else:
            ds3_bucket_name = self.bucket_name
        for driver in self.BACKUP_DRIVER:
            s3_key = s3_bucket.get_key(self.object_name)
            keyfile = KeyFile(s3_key)
            ds3_bucket = self.get_bucket(driver, ds3_bucket_name)
            key = Key(ds3_bucket)
            key.key = s3_key.key
            self.logger.info('Upload file %s to %s bucket %s',
                             self.file.file_name, driver, ds3_bucket_name)
            
            if self.reportfile:
                self.report[driver+'_start'] = time.time()

        try:
            if ds3_bucket.get_key(key.key):
                self.logger.info("File already exists, delete old one")
                ds3_bucket.delete_key(key.key)
            key.set_contents_from_file(keyfile,
                    md5=(self.file.md5sum, b64encode(unhexlify(self.file.md5sum))))
        except Exception as e:
            self.logger.error(str(e))
            ds3_bucket.delete_key(key.key)
            key.set_contents_from_file(keyfile)
        self.logger.info('File %s uploaded', self.file.file_name)
        if self.reportfile:
            self.report[driver+'_end'] = time.time()
        with self.graph.session_scope() as session:
            self.file.system_annotations[driver] = 'backuped'
            session.merge(self.file)


    def key_exists(self, bucket, key):
        if bucket.get_key(key):
            return True
        return False

    def get_bucket(self, driver, name):
        '''
        Get a bucket given a blackpearl driver name,
        if the bucket does not exist, create a new one
        '''
        bucket = self.ds3[driver].lookup(name)
        if not bucket:
            bucket = self.ds3[driver].create_bucket(name)
        return bucket


    def cleanup(self):
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
                for driver in self.BACKUP_DRIVER:
                    conditions.append(not_(File._sysan.has_key(driver)))
                    conditions.append((File._sysan[driver].astext
                                      == self.BACKUP_FAIL_STATE))
                query = self.graph.nodes(File).props({'state': 'live'})\
                    .filter(or_(*conditions))
                if query.count() == 0:
                    self.logger.info("We backed up all files")
                else:
                    tries = 5
                    while tries > 0:
                        tries -= 1
                        self.file = query.order_by(func.random()).first()
                        if self.get_consul_lock():
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
                if not self.get_consul_lock():
                    self.logger.error(
                        "Can't acquire lock for file {}".
                        format(self.file.file_name))
                    return
                return self.file
