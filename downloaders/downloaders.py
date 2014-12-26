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
import py2neo
import boto.s3.connection
from os import listdir
from os.path import isfile, join
from multiprocessing import Pool
from filechunkio import FileChunkIO
import math

logger = logging.getLogger(name="downloader")
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s][%(name)10s][%(levelname)7s] %(message)s'
)


mute = ['py2neo', 'httpstream', 'urllib3']
for mod in mute:
    logging.getLogger(mod).setLevel(logging.WARNING)


def which(program):
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None


def upload_multipart(s3_info, mpid, path, offset, bytes, index):
    conn = boto.connect_s3(
        aws_access_key_id=s3_info["s3_access_key"],
        aws_secret_access_key=s3_info["s3_secret_key"],
        host=s3_info["s3_url"],
        is_secure=False,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
    )
    bucket = conn.get_bucket(s3_info["s3_bucket"])
    for mp in bucket.list_multipart_uploads():
        if mp.id == mpid:
            logging.info("Posting part {}".format(index))
            with FileChunkIO(path, 'r', offset=offset, bytes=bytes) as f:
                mp.upload_part_from_file(f, index)
                logging.info("Posted part {}".format(index))
                return
    logging.error('Unable to find my mp.id [{}] in the mp id list: {}'.format(
        mpid, index))
    raise RuntimeError("Multipart upload {} not found.".format(mpid))


def no_proxy(func):
    def wrapped(*args, **kwargs):
        http_proxy = os.environ.get('http_proxy', None)
        https_proxy = os.environ.get('https_proxy', None)
        logger.info("no_proxy: " + str(func))
        logger.debug("Unsetting proxies")
        if http_proxy:
            del os.environ['http_proxy']
        if https_proxy:
            del os.environ['https_proxy']
        ret = func(*args, **kwargs)
        logger.debug("Resetting proxies: ")
        if http_proxy:
            os.environ['http_proxy'] = http_proxy
        if https_proxy:
            os.environ['https_proxy'] = https_proxy
        return ret
    return wrapped


class NoMoreWork(Exception):
    pass


class Downloader(object):
    """
    takes in an xml as a string and compiles a list of nodes and edges
    """

    def __init__(self, neo4j_host, neo4j_port, signpost_host,
                 signpost_port, s3_auth_path, s3_url, s3_bucket,
                 download_path, access_group, cghub_key, name,
                 extra_cypher='', no_work_delay=900, resume=True,
                 force_resume_id=None):

        self.state = 'IDLE'
        self.work = None
        self.bai = None
        self.files = []

        atexit.register(self.check_error)
        self.check_count = int(5)
        self.download_path = download_path
        self.cghub_key = cghub_key
        self.access_group = access_group
        self.extra_cypher = extra_cypher
        self.no_work_delay = no_work_delay
        self.resume = resume
        self.force_resume_id = force_resume_id

        self.load_name(name)
        self.load_logger()
        self.load_s3_settings(s3_auth_path, s3_url, s3_bucket)
        self.load_signpost_settings(signpost_host, signpost_port)
        self.load_neo4j_settings(neo4j_host, neo4j_port)

    def load_name(self, name):
        if not name:
            raise Exception('Name cant be empty or null')
        self.id = name

    def load_logger(self):
        self.logger = logging.getLogger(name=self.id)

    def load_neo4j_settings(self, neo4j_host, neo4j_port):
        self.logger.info('Loading neo4j settings')
        self.neo4j_url = 'http://{host}:{port}/db/data'.format(
            host=neo4j_host, port=neo4j_port)
        py2neo.rewrite(
            ("http", "0.0.0.0", 7474),
            ("http", neo4j_host, 7474)
        )
        self.graph = py2neo.Graph(self.neo4j_url)

    def load_signpost_settings(self, signpost_host, signpost_port):
        self.logger.info('Loading signpost settings')
        self.signpost_url = 'http://{host}:{port}/v0/'.format(
            host=signpost_host, port=signpost_port)

    def load_s3_settings(self, s3_auth_path, s3_url, s3_bucket):
        self.logger.info('Loading s3 settings')
        self.s3_url = s3_url
        self.s3_bucket = s3_bucket

        if not s3_auth_path:
            raise Exception('No path specified for s3 authentication')

        with open(s3_auth_path) as f:
            s3_auth = yaml.load(f.read().strip())
            self.s3_access_key = s3_auth['access_key']
            self.s3_secret_key = s3_auth['secret_key']

    def check_gtdownload(self):
        self.logger.info('Checking that genetorrent is installed')
        gtdownload = which('gtdownload')
        if not gtdownload:
            raise Exception(
                'Please make sure that gtdownload is installed/in your PATH.')
        self.logger.info('Using download client {}'.format(gtdownload))

    def check_download_path(self):
        self.logger.info('Checking that download_path exists')
        if not os.path.isdir(self.download_path):
            logging.error('Please make sure that your download path exists '
                          'and is a directory')
            raise Exception('{} does not exist'.format(self.download_path))
        self.logger.info('Downloading to {}'.format(self.download_path))

    def check_signpost(self):
        try:
            r = requests.get(self.signpost_url)
            if r.status_code != 500:
                logging.error('Signpost unreachable at {}'.format(
                    self.signpost_url))
        except Exception:
            logging.error('Signpost unreachable at {}'.format(
                self.signpost_url))
        else:
            self.logger.info('Found signpost at {}'.format(self.signpost_url))

    def check_neo4j(self):
        self.logger.info('Checking that neo4j is reachable')
        self.graph.cypher.execute('return timestamp()')
        self.logger.info('Found neo4j at {}'.format(self.neo4j_url))

    def check_s3(self):
        self.logger.info('Checking that s3 is reachable')
        r = requests.get('http://{}'.format(self.s3_url))
        if r.status_code != 200:
            logging.error('Status: {}'.format(r.status_code))
            raise Exception('s3 unreachable at {}'.format(
                self.s3_url))
        self.logger.info('Found s3 gateway at {}'.format(self.s3_url))

    def set_state(self, state):
        """Used to transition from one state to another"""

        self.state = state
        self.logger.info("Entering state: {0}".format(state))

        if self.work and 'id' in self.work:
            try:
                self.graph.cypher.execute("""
                    MATCH (n:file {{id:"{file_id}"}})
                    SET n.importer_state="{state}"
                """.format(file_id=self.work['id'], state=state))
            except:
                self.logger.error("Unable to update importer_state in neo4j")
                self.logger.error("Attempting to proceed regardless")
        self.logger.info("Entered state: {}".format(state))

    def sanity_checks(self):
        self.check_gtdownload()
        self.check_download_path()
        self.check_neo4j()
        self.check_s3()

    def start(self):
        self.sanity_checks()
        while True:
            if not self.do_carefully(self.resume_work):
                if not self.do_carefully(self.get_work):
                    continue
            if not self.do_carefully(self.download):
                continue
            if not self.do_carefully(self.checksum):
                continue
            if not self.do_carefully(self.upload):
                continue
            self.do_carefully(self.finish_work)
            self.check_error()

    def do_carefully(self, func):
        """wrapper to handle esceptions and catch interrupts"""

        try:
            retval = func()
        except KeyboardInterrupt:
            self.post_error('KeyboardInterrupt: Process was stopped by user')
            raise
        except NoMoreWork:
            self.check_error()
            logging.warn('No work found. Waiting {}s'.format(
                self.no_work_delay))
            time.sleep(self.no_work_delay)  # wait 20 minutes
            return False
        except Exception, msg:
            if not isinstance(Exception, KeyboardInterrupt):
                traceback.print_exc()
            logging.error('Downloader errored while executing {f}'.format(
                f=func))
            self.check_error(msg)
            time.sleep(3)
            return False
        return retval

    def resume_forced_id(self):
        self.set_state('RESUMING_ID')

        self.resume = False
        self.resume_forced_id = None

        results = self.graph.cypher.execute("""
        MATCH (n:file) WHERE n.id = "{uuid}"
        WITH n LIMIT 1 RETURN n
        """.format(uuid=self.force_resume_id))

        if not len(results):
            raise Exception('Unable to resume given id!')

        try:
            self.work = results[0].n.properties
        except:
            raise Exception('Unable get id from results!')

        self.logger.warn("Per your request, resuming file: {0}".format(
            self.work['id']))
        return True

    def resume_work(self):
        """resume an unfinished file"""

        if not self.resume:
            return False

        if self.force_resume_id:
            return self.resume_forced_id()

        self.set_state('RESUMING')
        self.resume = False

        results = self.graph.cypher.execute("""
        MATCH (n:file) WHERE n.import_state =~ "STARTED|ERROR"
        AND n.importer = "{name}"
        AND n.access_group[0] =~ "{a_group}"
        AND right(n.file_name, 4) <> ".bai" {extra}
        WITH n LIMIT 1 RETURN n
        """.format(
            a_group=self.access_group, extra=self.extra_cypher, name=self.id
        ))

        if not len(results):
            logging.warn('No file belonging to me found.')
            return False

        try:
            self.work = results[0].n.properties
        except:
            # failed to get work
            return False

        self.logger.warn("Resuming file: {0}".format(self.work['id']))
        return True

    def claim_work(self):
        """query the database for a file to download"""

        results = self.graph.cypher.execute("""
            MATCH (n:file) WHERE n.import_state="NOT_STARTED"
            AND n.access_group[0] =~ "{a_group}"
            AND right(n.file_name, 4) = ".bam" {extra}
            WITH n LIMIT 1 RETURN n
        """.format(a_group=self.access_group, extra=self.extra_cypher))
        if not len(results):
            self.set_state('EXITING')
            raise NoMoreWork('No More Work')

        file_id = results[0].n.properties['id']

        results = self.graph.cypher.execute("""
            MATCH (n:file {{id:"{file_id}"}})
            WHERE n.import_state="NOT_STARTED"
            SET n.importer="{id}",
            n.import_state="STARTED",
            n.import_started = timestamp()
            RETURN n
        """.format(file_id=file_id, id=self.id))

        try:
            self.work = results[0].n.properties
        except:
            # failed to get work
            return False

        for i in range(self.check_count):
            if not self.verify_claim(file_id):
                return False

        self.set_state('SCHEDULED')
        self.logger.info("Claimed: {0}".format(self.work['id']))
        return True

    def verify_claim(self, file_id):
        """verify that we're the only ones who claimed the data"""

        time.sleep(random.random()*3 + 1)
        result = self.graph.cypher.execute("""
            MATCH (n:file {{id:"{file_id}"}})
            WHERE n.importer="{id}"
            RETURN n
        """.format(file_id=file_id, id=self.id))

        if not len(result):
            return False
        return True

    def download(self):
        """download with genetorrent"""

        self.set_state('DOWNLOADING')
        if self.work is None:
            raise Exception('download() was called with no work')
        directory = self.download_path

        self.logger.info("Downloading file: {0} GB".format(
            self.work.get('file_size', 0)/1e9))

        cmd = ' '.join([
            'sudo gtdownload -v -k 15',
            '-c {cghub_key}',
            '-l {aid}.log',
            '--max-children 4',
            '-p {download_path}',
            '{aid}',
        ]).format(
            cghub_key=self.cghub_key,
            download_path=directory,
            aid=self.work.get('analysis_id'),
        )

        self.logger.info("cmd: {0}".format(cmd))
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        output, err = child.communicate()

        if child.returncode:
            raise Exception('Downloader returned with non-zero exit code')

        directory = os.path.join(directory, self.work.get('analysis_id'))
        files = [f for f in listdir(directory) if isfile(join(directory, f))]
        self.files = [
            os.path.join(directory, f) for f in files
            if f.endswith('.bam') or f.endswith('.bai')
        ]

        if len(self.files) < 1:
            raise Exception('Expected self.files to have files! Found none!')

        for f in self.files:
            self.logger.info('Successfully downloaded file: ' + f)

        self.set_state('DOWNLOADED')
        return True

    def get_bai(self):
        aid = self.work['analysis_id']
        self.logger.info("Got .bai file: " + aid)
        results = self.graph.cypher.execute("""
            MATCH (n:file)
            WHERE n.analysis_id="{aid}"
            AND right(n.file_name, 4) = ".bai"
            RETURN n
        """, dict(aid=aid))

        try:
            self.bai = results[0].n.properties
            return self.bai
        except:
            return None

    def checksum(self):
        self.set_state('CHECK_SUMMING')

        if len(self.files) < 1:
            raise Exception('Expected self.files to have files! Found none!')

        for path in self.files:

            self.logger.info("Checksumming file: {0}".format(path))

            work = self.get_bai() if path.endswith('.bai') else self.work
            if not work:
                continue

            cmd = '/bin/bash -c "md5sum -c <(echo {md5} {path})"'.format(
                md5=work['md5'],
                path=path,
            )

            self.logger.info("cmd: {0}".format(cmd))
            child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            output, err = child.communicate()

            if child.returncode:
                raise Exception(
                    'Checksum check returned with non-zero exit code')

        self.set_state('CHECK_SUMMED')
        return True

    def post_did(self, data):
        acls = data.get('access_group', [])
        base_url = "s3://{s3_url}/{s3_bucket}/{aid}/{name}"
        url = base_url.format(
            s3_url=self.s3_url,
            s3_bucket=self.s3_bucket,
            aid=data['analysis_id'],
            name=data['file_name']
        )
        data = {"acls": acls, "did": data['id'], "urls": [url]}
        r = requests.put(self.signpost_url, data=json.dumps(data),
                         headers={'Content-Type': 'application/json'})
        self.logger.info("Post: " + r.text)

    def post(self):
        self.set_state('POSTING')

        if len(self.files) < 1:
            raise Exception('Expected self.files to have files! Found none!')

        for path in self.files:
            if path.endswith('.bai'):
                self.post_did(self.bai)
            else:
                self.post_did(self.work)

        self.set_state('POSTED')
        return True

    @no_proxy
    def upload_file(self, data, path):

        name = path.replace(self.download_path, '')
        self.logger.info("Uploading file: " + path)
        self.logger.info("Uploading file to " + name)

        try:
            self.logger.info("Connecting to S3")
            conn = boto.connect_s3(
                aws_access_key_id=self.s3_access_key,
                aws_secret_access_key=self.s3_secret_key,
                host=self.s3_url,
                is_secure=False,
                calling_format=boto.s3.connection.OrdinaryCallingFormat(),
            )
        except Exception, msg:
            self.logger.error(msg)
            raise Exception("Unable to connect to s3 endpoint")
        else:
            self.logger.info("Connected to s3")

        try:
            block_size = 1073741824  # bytes (1 GiB) must be > 5 MB
            self.logger.info("Getting bucket")
            bucket = conn.get_bucket(self.s3_bucket)

            self.logger.info("Initiating multipart upload")
            mp = bucket.initiate_multipart_upload(name)
            self.logger.info("Initiated multipart upload: {}".format(mp.id))

            pool = Pool(processes=15)
            self.logger.info("Loading file")
            source_size = os.stat(path).st_size
            self.logger.info("File size is: {} GB".format(source_size/1e9))
            chunk_amount = int(math.ceil(source_size / float(block_size)))
            self.logger.info("Number of chunks: {}".format(chunk_amount))
            self.logger.info("Starting upload")
            s3_info = {
                "s3_access_key": self.s3_access_key,
                "s3_secret_key": self.s3_secret_key,
                "s3_url": self.s3_url,
                "s3_bucket": self.s3_bucket
            }
            for i in range(chunk_amount):
                # compute offset and bytes
                offset = i * block_size
                remaining_bytes = source_size - offset
                bytes = min([block_size, remaining_bytes])
                part_num = i + 1
                pool.apply_async(
                    upload_multipart,
                    [s3_info, mp.id, path, offset, bytes, part_num]
                )
            pool.close()
            pool.join()

            part_count = len(mp.get_all_parts())
            if part_count == chunk_amount:
                self.logger.info("Completing multipart upload")
                mp.complete_upload()
            else:
                mp.cancel_upload()
                raise RuntimeError(
                    "Multipart upload failure. Expected {} parts, found {}".format(
                        chunk_amount, part_count))

        except Exception as e:
            self.logger.error(e)
            raise e

        self.logger.info("Upload complete: " + path)
        return True

    def upload(self):
        self.set_state('UPLOADING')

        if len(self.files) < 1:
            raise Exception('Expected self.files to have files! Found none!')

        for path in self.files:
            if path.endswith('.bai'):
                self.upload_file(self.bai, path)
            else:
                self.upload_file(self.work, path)

        self.set_state('UPLOADED')
        return True

    def get_work(self):
        self.set_state('SCHEDULING')
        while not self.claim_work():
            self.logger.info("Failed to get work, trying again")
        return True

    def delete_scratch(self, path):
        self.set_state('DELETING_SCRATCH')
        directory = os.path.dirname(path)
        cmd = 'sudo rm -rf {directory}'.format(directory=directory)
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        output, err = child.communicate()
        if child.returncode:
            self.logger.error(err)
        return True

    def finish_work(self):
        self.set_state('FINISHING')
        self.graph.cypher.execute("""
            MATCH (n:file {{id:"{file_id}"}})
            SET n.import_state="COMPLETE",
            n.importer_state="FINISHED",
            n.import_completed = timestamp()
        """.format(file_id=self.work['id']))

        if len(self.files) < 1:
            raise Exception('Expected self.files to have files! Found none!')

        for f in self.files:
            try:
                self.delete_scratch(f)
            except:
                self.logger.error("Unable to delete scratch.  Will likely run "
                                  "out of space in the future")

        if self.bai:
            self.graph.cypher.submit("""
                MATCH (n:file {{id:"{file_id}"}})
                SET n.import_state="COMPLETE"
            """.format(file_id=self.bai['id']))

        self.set_state('IDLE')
        return True

    def post_error(self, msg="none"):
        msg = str(msg)
        self.logger.warn("Posting error state: " + msg)
        try:
            if not self.work:
                self.logger.error("No work to set to state ERROR.")
                return
            self.graph.cypher.execute("""
                MATCH (n:file {{id:"{file_id}"}})
                SET n.import_state="ERROR", n.error_msg="{msg}"
            """.format(file_id=self.work['id'], msg=msg))
            self.logger.warn("Successfully set file state to ERROR.")
        except Exception, msg:
            self.logger.error("UNABLE TO POST ERRORED STATE TO DATAMODEL !!")
            self.logger.error(str(msg))
        return True

    def check_error(self, msg='none'):
        self.logger.info("Checking for correct exit state, state = {0}".format(
            self.state))

        should_be_exiting = ['IDLE', 'EXITING']

        if self.state in should_be_exiting:
            self.logger.info("state okay.")
            return

        self.logger.error("EXITING WITH ERRORED STATE.")
        self.post_error(msg)
        self.work = None

        return True

if __name__ == '__main__':
    print('module downloaders has no __main__ functionality')
