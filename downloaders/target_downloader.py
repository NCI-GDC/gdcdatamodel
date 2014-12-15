
import os
import requests
import logging
import yaml
import traceback
import time
import json
import random
import atexit
import subprocess
import boto
from cStringIO import StringIO
import boto.s3.connection
from os import listdir
from os.path import isfile, join


logger = logging.getLogger(name="{name}".format(name=__file__))


def no_proxy(func):
    def wrapped(*args, **kwargs):
        http_proxy = os.environ.get('http_proxy', None)
        https_proxy = os.environ.get('https_proxy', None)

        logger.info("no_proxy: " + str(func))

        if http_proxy:
            logger.debug("Unsetting http_proxy")
            del os.environ['http_proxy']

        if https_proxy:
            logger.debug("Unsetting https_proxy")
            del os.environ['https_proxy']

        ret = func(*args, **kwargs)

        if http_proxy:
            logger.debug("Resetting http_proxy: " + http_proxy)
            os.environ['http_proxy'] = http_proxy

        if https_proxy:
            logger.debug("Resetting https_proxy: " + https_proxy)
            os.environ['https_proxy'] = https_proxy

        return ret
    return wrapped


class NoMoreWork(Exception):
    pass


class TCGADownloader(object):

    """
    takes in an xml as a string and compiles a list of nodes and edges
    """

    def __init__(self, cghub_key, ):
        self.state = 'IDLE'
        self.work = None
        self.bai = None
        self.files = []

        atexit.register(self.check_error)
        self.check_count = int(kwargs.get('check_count', 5))

        self.load_name()
        self.load_s3_settings()
        self.load_signpost_settings()
        self.load_neo4j_settings()

    def load_neo4j_settings(self):
        logger.info('Loading neo4j settings')
        self.url = 'http://{host}:{port}/db/data/cypher'.format(
            host=self.kwargs.get('neo4j', 'neo4j'),
            port=self.kwargs.get('port', '7474')
        )

    def load_signpost_settings(self):
        logger.info('Loading signpost settings')
        host = self.kwargs.get('signpost', 'signpost')
        sp_port = self.kwargs.get('sp_port', '8080')
        self.signpost = 'http://{host}:{port}/v0/'.format(
            host=host, port=sp_port)

    def load_name(self):
        logger.info('Loading name')
        with open('/etc/tungsten/name') as f:
            self.id = f.read().strip()

    def load_s3_settings(self):
        logger.info('Loading s3 settings')
        s3_auth_path = self.kwargs.get('s3_auth_path', None)
        self.s3_url = self.kwargs.get('s3_url', 's3')

        if not s3_auth_path:
            raise Exception('No path specified for s3 authentication')

        with open(s3_auth_path) as f:
            s3_auth = yaml.load(f.read().strip())
            self.s3_access_key = s3_auth['access_key']
            self.s3_secret_key = s3_auth['secret_key']

    def set_state(self, state):
        self.state = state
        logger.info("Entering state: {0}".format(state))

        if self.work and 'id' in self.work:
            try:
                self.submit([
                    'MATCH (n:file {{id:"{file_id}"}})',
                    'SET n.importer_state="{state}"',
                ], file_id=self.work['id'], state=state)
            except:
                logger.error("Unable to update importer_state in datamodel")
        logger.info("Entered state: {0}".format(state))

    def start(self):
        while True:
            self.do_carefully(self.get_work)
            self.do_carefully(self.download)
            self.do_carefully(self.checksum)
            self.do_carefully(self.upload)
            self.do_carefully(self.post)
            self.do_carefully(self.finish_work)
            self.check_error()

    def do_carefully(self, func):
        try:
            func()
        except KeyboardInterrupt:
            self.post_error('KeyboardInterrupt: Process was stopped by user')
            raise
        except NoMoreWork:
            self.check_error()
        except Exception, msg:
            traceback.print_exc()
            logging.error('Downloader errored while executing {f}'.format(
                f=func))
            self.check_error(msg)
            time.sleep(2)

    def verify_claim(self, file_id):
        time.sleep(random.random()*8 + 1)
        result = self.submit([
            'MATCH (n:file {{id:"{file_id}"}})',
            'WHERE n.importer="{id}"',
            'RETURN n',
        ], file_id=file_id, id=self.id)

        if len(result['data']) == 0:
            return False
        return True

    def claim_work(self):
        result = self.submit([
            'MATCH (n:file)',
            'WHERE n.import_state="NOT_STARTED"'
            'AND n.access_group[0] =~ "phs0004(64|65|66|67|68|69|71)"',
            'AND right(n.file_name, 4) <> ".bai"',
#            'AND n.file_size/1000000000 < 150', # GB
            'WITH n LIMIT 1',
            'RETURN n',
            ])

        if not len(result['data']):
            self.set_state('EXITING')
            raise NoMoreWork('No More Work')

        file_id = result['data'][0][0]['data']['id']

        result = self.submit([
            'MATCH (n:file {{id:"{file_id}"}})',
            'WHERE n.import_state="NOT_STARTED"'
            'SET n.importer="{id}", ',
            'n.import_state="STARTED", ',
            'n.import_started = timestamp()',
            'RETURN n',
        ], file_id=file_id, id=self.id)

        try:
            self.work = result['data'][0][0]['data']
        except:
            return False

        for i in range(self.check_count):
            if not self.verify_claim(file_id):
                return False

        self.set_state('SCHEDULED')
        logger.info("Claimed: {0}".format(self.work['id']))
        return True

    def delete_scratch(self, path):

        directory = os.path.dirname(path)
        cmd = 'sudo rm -rf {directory}'.format(directory=directory)
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        output, err = child.communicate()
        if child.returncode:
            logger.error(err)

    def finish_work(self):
        self.set_state('FINISHING')
        self.submit([
            'MATCH (n:file {{id:"{file_id}"}})',
            'SET n.import_state="COMPLETE", ',
            'n.importer_state="FINISHED",',
            'n.import_completed = timestamp()',
        ], file_id=self.work['id'])

        for f in self.files:
            try:
                self.delete_scratch(f)
            except:
                logger.error("Unable to delete scratch.  Will likely run out"
                             " of space in the future")
        if not self.bai:
            return

        self.submit([
            'MATCH (n:file {{id:"{file_id}"}})',
            'SET n.import_state="COMPLETE"',
        ], file_id=self.bai['id'])
        self.set_state('IDLE')

    def download(self):
        self.set_state('DOWNLOADING')
        if self.work is None:
            raise Exception('download() was called with no work')
        directory = self.kwargs['download_path']

        logger.info("Downloading file: {0} bytes".format(
            self.work.get('file_size', '?')))

        cmd = ' '.join([
            'sudo gtdownload -k 15',
            '-c {cghub_key}',
            '-l {aid}.log',
            '-p {download_path}',
            '{aid}',
        ]).format(
            cghub_key=self.kwargs['cghub_key'],
            download_path=directory,
            aid=self.work.get('analysis_id'),
        )

        logger.info("cmd: {0}".format(cmd))
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        output, err = child.communicate()

        if child.returncode:
            raise Exception('Downloader returned with non-zero exit code')

        directory = os.path.join(directory, self.work.get('analysis_id'))
        files = [f for f in listdir(directory) if isfile(join(directory, f))]
        self.files = [
            os.path.join(directory, f) for f in files if f.endswith('.bam') or f.endswith('.bai')
        ]
        for f in self.files:
            logger.info('Successfully downloaded file: ' + f)

        self.set_state('DOWNLOADED')

    def get_bai(self):
        aid = self.work['analysis_id']
        logger.info("Got .bai file: " + aid)
        result = self.submit([
            'MATCH (n:file)',
            'WHERE n.analysis_id="{aid}"',
            'AND right(n.file_name, 4) = ".bai"',
            'RETURN n',
        ], aid=aid)

        try:
            self.bai = result['data'][0][0]['data']
            return self.bai
        except:
            return None

    def checksum(self):
        self.set_state('CHECK_SUMMING')
        for path in self.files:

            logger.info("Checksumming file: {0}".format(path))

            work = self.get_bai() if path.endswith('.bai') else self.work
            if not work:
                continue

            cmd = ' '.join([
                '/bin/bash -c '
                '"md5sum -c <(echo {md5} {path})"',
            ]).format(
                md5=work['md5'],
                path=path,
            )

            logger.info("cmd: {0}".format(cmd))
            child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            output, err = child.communicate()

            if child.returncode:
                raise Exception('Checksum check returned with non-zero exit code')

        self.set_state('CHECK_SUMMED')

    def post_did(self, data):
        acls = data.get('access_group', [])
        protection = "protected" if len(acls) else "public"
        base_url = "s3://{url}/target_cghub_{protection}/{aid}/{name}"
        url = base_url.format(url=self.s3_url, protection=protection,
                              aid=data['analysis_id'],
                              name=data['file_name'])
        data = {"acls": acls, "did": data['id'], "urls": [url]}
        r = requests.put(self.signpost, data=json.dumps(data),
                         headers={'Content-Type': 'application/json'})
        logger.info("Post: " + r.text)

    def post(self):
        self.set_state('POSTING')

        for path in self.files:
            if path.endswith('.bai'):
                self.post_did(self.bai)
            else:
                self.post_did(self.work)

        self.set_state('POSTED')

    @no_proxy
    def upload_file(self, data, path):

        name = path.replace(self.kwargs['download_path'], '')
        logger.info("Uploading file: " + path)
        logger.info("Uploading file to " + name)

        try:
            logger.info("Connecting to S3")
            conn = boto.connect_s3(
                aws_access_key_id=self.s3_access_key,
                aws_secret_access_key=self.s3_secret_key,
                host=self.s3_url,
                is_secure=False,
                calling_format=boto.s3.connection.OrdinaryCallingFormat(),
            )
        except Exception, msg:
            logger.error(msg)
            raise Exception("Unable to connect to s3 endpoint")
        else:
            logger.info("Connected to s3")

        try:
            block_size = 1073741824  # bytes (1 GiB) must be > 5 MB
            logger.info("Getting bucket")
            bucket = conn.get_bucket('target_cghub_protected')

            logger.info("Initiating multipart upload")
            mp = bucket.initiate_multipart_upload(name)

            logger.info("Loading file")
            with open(path, 'rb') as f:
                index = 1
                logger.info("Starting upload")
                for chunk in iter(lambda: f.read(block_size), b''):
                    logger.info("Posting part {0}".format(index))
                    mp.upload_part_from_file(StringIO(chunk), index)
                    logger.info("Posted part {0}".format(index))
                    index += 1

            logger.info("Completing multipart upload")
            mp.complete_upload()

        except Exception, msg:
            logger.error(msg)
            raise Exception("Unable to upload to s3")

        logger.info("Upload complete: " + path)


    def upload(self):
        self.set_state('UPLOADING')

        for path in self.files:
            if path.endswith('.bai'):
                self.upload_file(self.bai, path)
            else:
                self.upload_file(self.work, path)

        self.set_state('UPLOADED')

    def get_work(self):
        self.set_state('SCHEDULING')
        while not self.claim_work():
            logger.info("Failed to get work, trying again")

    @no_proxy
    def submit(self, cypher, **params):
        if isinstance(cypher, list):
            cypher = ' '.join(cypher).format(**params)
        data = {"query": cypher}
        r = requests.post(self.url, data=json.dumps(data))
        if r.status_code != 200:
            traceback.print_exc()
            logger.error("FAILURE: cypher query")
            logger.error(r.text)
        return r.json()

    def post_error(self, msg="none"):
        msg = str(msg)
        logger.warn("Posting error state: " + msg)
        try:
            if not self.work:
                logger.error("No work to set to state ERROR.")
                return
            self.submit([
                'MATCH (n:file {{id:"{file_id}"}})',
                'SET n.import_state="ERROR", n.error_msg="{msg}"',
            ], file_id=self.work['id'], msg=msg)
            logger.warn("Successfully set file state to ERROR.")
        except Exception, msg:
            logger.error("UNABLE TO POST ERRORED STATE TO DATAMODEL !!")
            logger.error(str(msg))

    def check_error(self, msg='none'):
        logger.info("Checking for correct exit state, state = {0}".format(
            self.state))
        should_be_exiting = ['IDLE', 'EXITING']

        if self.state in should_be_exiting:
            logger.info("state okay.")
            return
        logger.error("EXITING WITH ERRORED STATE.")
        self.post_error(msg)

        for f in self.files:
            try:
                self.delete_scratch(f)
            except:
                logger.error("Unable to delete scratch.  Will likely run out "
                             "of space in the future.")
        self.work = None

if __name__ == '__main__':

    home = os.path.expanduser('~')
    logging.basicConfig(level=logging.INFO)
    downloader = TCGADownloader(
        cghub_key=os.path.join(home, 'authorization/jmiller_cghub_key'),
        download_path='/mnt/rbd/scratch/',
        s3_auth_path=os.path.join(home, 'authorization/gdc.yaml'),
        signpost='localhost',
        sp_port='8080',
        neo4j='localhost',
        s3_url='192.170.230.172',
    )
    downloader.start()
