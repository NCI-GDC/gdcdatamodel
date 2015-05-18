from consul_mixin import ConsulMixin
from signpostclient import SignpostClient
from gdcdatamodel.models import File
from psqlgraph import PsqlGraphDriver
from cdisutils.log import get_logger
import os
from urlparse import urlparse
from requests.exceptions import HTTPError
import ds3client as ds3
import boto
import boto.s3.connection
import subprocess
from sqlalchemy import or_, not_
import requests

try:
    requests.packages.urllib3.disable_warnings()
except:
    pass


class DataBackup(ConsulMixin):
    def __init__(self, file_id='', bucket_prefix='', debug=False):
        super(DataBackup, self).__init__()
        self.graph = PsqlGraphDriver(
            os.environ["ZUGS_PG_HOST"],
            os.environ["ZUGS_PG_USER"],
            os.environ["ZUGS_PG_PASS"],
            os.environ["ZUGS_PG_NAME"],
        )
        self.ds3 = ds3.client.Client(
            os.environ["DS3_HOST"],
            os.environ["DS3_PORT"],
            access_key=os.environ["DS3_ACCESS_KEY"],
            secret_key=os.environ["DS3_SECRET_KEY"],
            verify=False
        )
        self.download_path = self.consul_get("path")
        self.s3 = boto.connect_s3(
            host=os.environ["S3_HOST"],
            aws_access_key_id=os.environ["S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["S3_SECRET_KEY"],
            is_secure=False,
            calling_format=boto.s3.connection.OrdinaryCallingFormat()
        )
        self.signpost = SignpostClient(self.consul_get('signpost_url'))
        self.logger = get_logger('data_backup_{}'.format(str(os.getpid())))
        self.debug=debug
        if not debug:
            self.logger.level=30
        self.file_id = file_id
        self._bucket_prefix = bucket_prefix

    @property
    def key(self):
        return self.file.node_id

    def backup(self):
        with self.consul_session_scope():
            if not self.get_file_to_backup():
                return
            if self.download():
                self.upload()

    def download(self):
        urls = self.signpost.get(self.file.node_id).urls
        if not urls:
            self.logger.error(
                "no urls in signpost for file {}".format(self.file.file_name))
            return False
        parsed_url = urlparse(urls[0])
        (self.bucket_name, self.object_name) =\
            parsed_url.path.split("/", 2)[1:]
        s3_bucket = self.s3.get_bucket(self.bucket_name)
        free = self.get_free_space()
        if free < self.file.file_size:
            self.logger.error(
                "Not enough space to download file %s File size is %s, free space is %s",
                self.file.file_name, self.file.file_size, free)
            return False
        else:
            key = s3_bucket.get_key(self.object_name)
            self.logger.info("Downloading file %s", self.file.file_name)
            key.get_contents_to_filename(
                os.path.join(self.download_path, self.file.node_id))
            self.logger.info("Download file %s complete", self.file.file_name)
            return True

    def upload(self):
        ds3_bucket_name = self._bucket_prefix+'_'+self.bucket_name
        self.get_bucket(ds3_bucket_name)
        f = open(os.path.join(self.download_path, self.file.node_id), 'r')
        self.logger.info('Upload file %s to blackpearl', self.file.file_name)
        try:
            self.ds3.keys.put(f, self.object_name, ds3_bucket_name)
        except HTTPError:
            self.ds3.keys.delete(self.object_name, ds3_bucket_name)
            self.ds3.keys.put(f, self.object_name, ds3_bucket_name)
        self.logger.info('File %s uploaded', self.file.file_name)

    def cleanup(self):
        intermediate = os.path.join(self.download_path, self.file_id)
        if os.path.exists(intermediate):
            os.remove(intermediate)
        super(DataBackup, self).cleanup()

    def get_free_space(self):
        output = subprocess.check_output(["df", self.download_path])
        device, size, used, available, percent, mountpoint = \
            output.split("\n")[1].split()
        return 1000 * int(available)  # 1000 because df reports in kB

    def get_bucket(self, name):
        try:
            return self.ds3.buckets.get(name)
        except HTTPError:
            try:
                self.ds3.buckets.create(name)
            except:
                pass
            return self.ds3.buckets.get(name)

    def get_file_to_backup(self):
        if not self.file_id:
            with self.graph.session_scope():
                query = self.graph.nodes(File).props({'state': 'live'})\
                    .filter(or_(not_(File._sysan.has_key('backup')),
                                File._sysan['backup'].astext == 'failed'))
                if query.count() == 0:
                    self.logger.info("We backed up all files")
                else:
                    tries = 5
                    while tries > 0:
                        tries -= 1
                        self.file = query.first()
                        if self.get_consul_lock():
                            self.file_id = self.file.node_id
                            return self.file
                    self.logger.error(
                        "Can't acquire lock after 5 tries")
        else:
            with self.graph.session_scope():
                self.file = self.graph.nodes(File).ids(self.file_id).one()
                if not self.get_consul_lock():
                    self.logger.error(
                        "Can't acquire lock for file {}".
                        format(self.file.file_name))
                    return None
                return self.file
