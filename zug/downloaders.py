import os
import socket
import subprocess
import signal
import time
import math
import calendar
from mmap import mmap, PROT_READ, PAGESIZE
from multiprocessing import Pool
from contextlib import contextmanager
from urlparse import urlparse
import shutil
import hashlib
from threading import Thread, Event, current_thread

from consulate import Consul

from filechunkio import FileChunkIO

import boto
import boto.s3.connection

from sqlalchemy import func
from sqlalchemy.exc import OperationalError

from cdisutils.log import get_logger
from psqlgraph import PsqlGraphDriver, Node
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from signpostclient import SignpostClient
from gdcdatamodel import node_avsc_object, edge_avsc_object


# buffer 10 MB in memory at once
from boto.s3.key import Key
Key.BufferSize = 10 * 1024 * 1024


class StoppableThread(Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self, *args, **kwargs):
        super(StoppableThread, self).__init__(*args, **kwargs)
        self._stop = False

    def stop(self):
        self._stop = True

    def stopped(self):
        return self._stop


class InvalidChecksumException(Exception):
    pass


def md5sum_with_size(iterable):
    md5 = hashlib.md5()
    size = 0
    for chunk in iterable:
        md5.update(chunk)
        size += len(chunk)
    return md5.hexdigest(), size


def consul_get(consul, path):
    return consul.kv["/".join(path)]


class TimeoutError(Exception):
    pass


class timeout:
    def __init__(self, seconds=1, msg="Timed out!"):
        self.seconds = seconds
        self.error_message = msg

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)


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


def upload_multipart_wrapper(args):
    """This exists purely to get around pickling problems with
    multiprocessing.Pool"""
    return upload_multipart(*args)


def upload_multipart(s3_info, key_name, mpid, path, offset, bytes, index):
    log = get_logger("{}_upload_part_{}".format(key_name, index))
    tries = 0
    while tries < 30:
        try:
            tries += 1
            log.info("try number {} to upload part {}".format(tries, index))
            with timeout(seconds=5*60, msg="Timed out uploading part {}, retrying".format(index)):
                # Reconnect to s3
                conn = boto.connect_s3(
                    aws_access_key_id=s3_info["access_key"],
                    aws_secret_access_key=s3_info["secret_key"],
                    host=s3_info["host"],
                    port=s3_info["port"],
                    is_secure=False,
                    calling_format=boto.s3.connection.OrdinaryCallingFormat(),
                )

                # Create a matching mp upload instead of looking it up
                bucket = conn.get_bucket(s3_info["bucket"])
                mp = boto.s3.multipart.MultiPartUpload(bucket)
                mp.key_name, mp.id = key_name, mpid

                # Upload this segment
                log.info("Posting part {}".format(index))
                if bytes % PAGESIZE == 0:
                    log.info("chunk size is %s, mmaping chunk", bytes)
                    f = open(path, "r+b")
                    chunk_file = mmap(
                        fileno=f.fileno(),
                        length=bytes,
                        offset=offset,
                        prot=PROT_READ
                    )
                else:
                    log.info("chunk size is %s, not a multiple of page size, reading with FileChunkIO", bytes)
                    chunk_file = FileChunkIO(path, "r", offset=offset, bytes=bytes)
                start = calendar.timegm(time.gmtime())
                mp.upload_part_from_file(chunk_file, index)
                stop = calendar.timegm(time.gmtime())
                chunk_file.close()
                els = stop - start
                log.info("Posted part {} {} MBps".format(
                    index, (bytes/float(1024*1024))/(els+0.001)))  # sometimes this gives division by zero?
                return
        except KeyboardInterrupt:
            raise
        except:
            log.exception("Caught exception while uploading, retrying in a second")
            time.sleep(1)
    log.error("Exhausted 30 retries, failing upload")
    raise RuntimeError("Retries exhausted, upload failed")


def consul_heartbeat(session, interval):
    """
    Heartbeat with consul to keep `session` alive every `interval`
    seconds. This must be called as the `target` of a `StoppableThread`.
    """
    consul = Consul()
    logger = get_logger("consul_heartbeat_thread")
    thread = current_thread()
    logger.info("current thread is %s", thread)
    while not thread.stopped():
        logger.debug("renewing consul session %s", session)
        consul.session.renew(session)
        time.sleep(interval)


class Downloader(object):

    def consul_get(self, path):
        return consul_get(self.consul, ["downloaders"] + path)

    @property
    def consul_key(self):
        return "downloaders/current/{}".format(self.analysis_id)

    def start_consul_session(self):
        self.logger.info("Starting new consul session")
        self.consul_session = self.consul.session.create(
            behavior="delete",
            ttl="60s",
            delay="0s"
        )
        self.logger.info("Consul session %s started, forking thread to heartbeat", self.consul_session)
        self.heartbeat_thread = StoppableThread(target=consul_heartbeat,
                                                args=(self.consul_session, 10))
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    def __init__(self, source=None, analysis_id=None):
        self.logger = get_logger("downloader_{}".format(socket.gethostname()))
        if not source:
            raise RuntimeError("Must specify a source")
        self.source = source
        assert self.source.endswith("_cghub")
        self.analysis_id = analysis_id

        self.consul = Consul()

        self.signpost_url = self.consul_get(["signpost_url"])
        self.signpost = SignpostClient(self.signpost_url, version="v0")

        self.pg_info = {}
        self.pg_info["host"] = self.consul_get(["pg", "host"])
        self.pg_info["user"] = self.consul_get(["pg", "user"])
        self.pg_info["name"] = self.consul_get(["pg", "name"])
        self.pg_info["pass"] = self.consul_get(["pg", "pass"])
        self.graph = PsqlGraphDriver(self.pg_info["host"], self.pg_info["user"],
                                     self.pg_info["pass"], self.pg_info["name"])
        self.graph.node_validator = AvroNodeValidator(node_avsc_object)
        self.graph.edge_validator = AvroEdgeValidator(edge_avsc_object)
        self.s3_info = {}
        self.s3_info["host"] = self.consul_get(["s3", "host"])
        self.s3_info["port"] = int(self.consul_get(["s3", "port"]))
        self.s3_info["access_key"] = self.consul_get(["s3", "access_key"])
        self.s3_info["secret_key"] = self.consul_get(["s3", "secret_key"])
        self.s3_info["bucket"] = self.consul_get(["s3", "buckets", self.source])
        self.setup_s3()
        self.download_path = self.consul_get(["path"])

    def setup_s3(self):
        self.logger.info("Connecting to s3 at %s.", self.s3_info["host"])
        self.boto_conn = boto.connect_s3(
            aws_access_key_id=self.s3_info["access_key"],
            aws_secret_access_key=self.s3_info["secret_key"],
            host=self.s3_info["host"],
            port=self.s3_info["port"],
            is_secure=False,
            calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        )
        self.logger.info("Getting s3 bucket %s", self.s3_info["bucket"])
        self.s3_bucket = self.boto_conn.get_bucket(self.s3_info["bucket"])

    def check_gtdownload(self):
        self.logger.info('Checking that genetorrent is installed')
        gtdownload = which('gtdownload')
        if not gtdownload:
            raise RuntimeError('Please make sure that gtdownload is installed/in your PATH.')
        self.logger.info('Using download client {}'.format(gtdownload))

    def check_download_path(self):
        self.logger.info('Checking that download_path exists')
        if not os.path.isdir(self.download_path):
            raise RuntimeError('{} does not exist'.format(self.download_path))
        self.logger.info('Downloading to {}'.format(self.download_path))

    def sanity_checks(self):
        self.check_gtdownload()
        self.check_download_path()

    def get_files_to_download(self):
        self.logger.info("Finding file to download")
        tries = 0
        while tries < 5:
            tries += 1
            if not self.analysis_id:
                start_file = self.graph.nodes()\
                                       .labels("file")\
                                       .props({"state": "submitted"})\
                                       .sysan({"source": self.source})\
                                       .filter(func.right(Node.properties["file_name"].astext, 4) != ".bai")\
                                       .order_by(func.random())\
                                       .first()
                self.analysis_id = start_file.system_annotations["analysis_id"]
            try:
                # attempt to acquire a lock on all the files with this analysis id
                files = self.graph.nodes()\
                                  .labels("file")\
                                  .sysan({
                                      "source": self.source,
                                      "analysis_id": self.analysis_id
                                  }).with_for_update(nowait=True).all()
                # also lock the relevant consul key
                locked = self.consul.kv.acquire_lock(self.consul_key, self.consul_session)
                if not locked:
                    raise RuntimeError("Couldn't lock consul key {}!".format(self.consul_key))
                self.consul.kv.set(
                    self.consul_key,
                    {
                        "host": socket.gethostname(),
                        "started": self.start_time
                    }
                )
                self.set_consul_state("downloading")
                if not files:
                    raise RuntimeError(
                        "File with analysis id {} seems to have disappeared, something is very wrong".format(
                            start_file.system_annotations["analysis_id"])
                    )
                else:
                    for file in files:
                        assert file.system_annotations["source"] == self.source
                        assert file["state"] == "submitted"
                    self.files = files
                self.logger.info("Found %s files (%s) with analysis_id %s", len(files), files, self.analysis_id)
                return self.files
            except OperationalError:
                self.graph.current_session().rollback()
                self.logger.exception("Caught OperationalError on try %s to find files to download, retrying", tries)
                time.sleep(3)
        raise RuntimeError("Couldn't find files to download in five tries")

    def get_free_space(self):
        output = subprocess.check_output(["df", self.download_path])
        device, size, used, available, percent, mountpoint = output.split("\n")[1].split()
        return 1000 * int(available)  # 1000 because df reports in kB

    def call_gtdownload(self):
        cmd = ' '.join([
            'gtdownload -v -k 15',
            '-c /var/tungsten/keys/cghub',
            '-l {aid}.log',
            '--max-children 4',
            '-p {download_path}',
            '{aid}',
        ]).format(
            download_path=self.download_path,
            aid=self.analysis_id,
        )
        self.logger.info("genetorrent command: {0}".format(cmd))
        subprocess.check_call(cmd, shell=True)

    def download(self):
        """Download self.analysis_id with genetorrent"""
        self.logger.info("Checking free space")
        free = self.get_free_space()
        total_file_size = sum([file["file_size"] for file in self.files])
        self.logger.info("%s bytes free, total files size is %s", free, total_file_size)
        if free < total_file_size:
            self.logger.critical("Not enough space to download file! File size is %s, free space is %s", file_size, free)
            exit(1)
        self.logger.info("Downloading files {}, total {} GB".format(self.files, total_file_size/1e9))
        self.call_gtdownload()
        download_directory = os.path.join(self.download_path,
                                          self.analysis_id)
        paths = [f for f in os.listdir(download_directory)
                 if os.path.isfile(os.path.join(download_directory, f))]
        self.paths = [os.path.join(download_directory, f) for f in paths]

        if len(self.paths) != len(self.files):
            raise RuntimeError('Number of files downloaded from genetorrent ({}) is not what was expected ({})'.format(len(self.paths), len(self.files)))

        for f in self.paths:
            self.logger.info('Successfully downloaded file: %s', f)
        return self.paths

    def upload_file(self, file, path):
        assert file.system_annotations["analysis_id"] == self.analysis_id
        name = "/".join([self.analysis_id, file["file_name"]])
        assert path.endswith(name)
        self.logger.info("Uploading file: " + path)
        self.logger.info("Uploading file to " + name)

        block_size = 1073741824  # bytes (1 GiB) must be > 5 MB

        self.logger.info("Initiating multipart upload in bucket %s", self.s3_bucket.name)
        mp = self.s3_bucket.initiate_multipart_upload(name)
        self.logger.info("Initiated multipart upload: {}".format(mp.id))
        time.sleep(5)  # give cleversafe time to have the upload show up everywhere
        pool = Pool(processes=5)
        self.logger.info("Loading file")
        source_size = os.stat(path).st_size
        self.logger.info("File size is: {} GB".format(source_size/1e9))
        chunk_amount = int(math.ceil(source_size / float(block_size)))
        self.logger.info("Number of chunks: {}".format(chunk_amount))
        self.logger.info("Starting upload")
        args_list = []
        for i in range(chunk_amount):
            # compute offset and bytes
            offset = i * block_size
            remaining_bytes = source_size - offset
            bytes = min([block_size, remaining_bytes])
            part_num = i + 1
            args_list.append([
                self.s3_info, mp.key_name, mp.id,
                path, offset, bytes,
                part_num
            ])
        pool.map_async(upload_multipart_wrapper, args_list).get(99999999)
        pool.close()
        pool.join()

        part_count = len(mp.get_all_parts())
        if part_count == chunk_amount:
            self.logger.info("Completing multipart upload")
            mp.complete_upload()
        else:
            self.logger.error("Multipart upload failed, expected %s parts, found %s", chunk_amount, part_count)
            mp.cancel_upload()
            raise RuntimeError(
                "Multipart upload failure. Expected {} parts, found {}".format(
                    chunk_amount, part_count))

        self.logger.info("Upload complete: " + path)

    def set_file_state(self, file, state):
        self.graph.node_update(file, properties={"state": state})

    def set_consul_state(self, state):
        current = self.consul.kv.get(self.consul_key)
        current["state"] = state
        self.logger.info("Setting %s to %s", self.consul_key, current)
        self.consul.kv.set(self.consul_key, current)

    @contextmanager
    def state_transition(self, file, intermediate_state, final_state,
                         error_states={}):
        """Try to do something to a file, setting it's state to
        intermediate_state while the thing is being done, moving to
        final_state if the thing completes successfully, falling back to the original
        state if the thing fails
        """
        original_state = file["state"]
        self.logger.info("Attempting to move %s to %s via %s", file, final_state, intermediate_state)
        try:
            self.set_file_state(file, intermediate_state)
            yield
            self.set_file_state(file, final_state)
        except BaseException as e:
            for err_cls, state in error_states.iteritems():
                if isinstance(e, err_cls):
                    self.logger.warning("%s caught, setting %s to %s", err_cls.__name__, file, state)
                    self.set_file_state(file, state)
                    return
            self.logger.exception("failure while trying to move %s from %s to %s via %s",
                                  file, original_state, final_state, intermediate_state)
            self.set_file_state(file, original_state)
            raise e

    def url_for_file(self, file):
        template = "s3://{host}/{bucket}/{analysis_id}/{file_name}"
        return template.format(
            host=self.s3_bucket.connection.host,
            bucket=self.s3_bucket.name,
            analysis_id=self.analysis_id,
            file_name=file["file_name"]
        )

    def upload(self, file):
        path = os.path.join(self.download_path, self.analysis_id, file["file_name"])
        assert os.path.isfile(path)
        self.logger.info("File %s is at path %s, uploading to S3", file, path)
        self.upload_file(file, path)
        self.logger.info("Finding file %s in signpost", file)
        doc = self.signpost.get(file.node_id)
        url = self.url_for_file(file)
        self.logger.info("Patching file %s with url %s", file, url)
        doc.urls = [url]
        doc.patch()

    def delete_scratch(self):
        complete_dir = os.path.join(self.download_path, self.analysis_id)
        partial_dir = os.path.join(self.download_path, self.analysis_id + ".partial")
        self.logger.info("Checking for directories to destroy")
        for dir in [complete_dir, partial_dir]:
            if os.path.isdir(dir):
                self.logger.info("Removing directory %s", dir)
                shutil.rmtree(dir)

    def cleanup(self):
        self.logger.info("Cleaning up before shutting down")
        self.delete_scratch()
        self.logger.info("Stopping consul heartbeat thread")
        self.heartbeat_thread.stop()
        self.logger.info("Waiting to join heartbeat thread . . .")
        self.heartbeat_thread.join(20)
        if self.heartbeat_thread.is_alive():
            self.logger.warning("Joining heartbeat thread failed after 20 seconds!")
        self.logger.info("Invalidating consul session")
        self.consul.session.destroy(self.consul_session)

    def verify(self, file):
        self.logger.info("Reconstructing boto key for %s from signpost url", file)
        url = self.signpost.get(file.node_id).urls[0]
        self.logger.info("signpost url: %s", url)
        parsed = urlparse(url)
        assert parsed.scheme == "s3"
        assert parsed.netloc == self.s3_info["host"]
        bucket, name = parsed.path.split("/", 2)[1:]
        assert bucket == self.s3_bucket.name
        key = self.s3_bucket.get_key(name)
        self.logger.info("Computing md5sum from boto key %s", key)
        boto_key_size = int(key.size)
        if boto_key_size != int(file["file_size"]):
            self.logger.warning(
                "file size in database (%s) does not match key size in S3 (%s)",
                file["file_size"],
                boto_key_size,
            )
            raise InvalidChecksumException()
        tries = 0
        while tries < 5:
            tries += 1
            md5, actual_stream_size = md5sum_with_size(key)
            if actual_stream_size != boto_key_size:
                self.logger.warning("Actual size streamed from S3 (%s) did not equal expected size according to boto (%s), retrying (%s tries so far).",
                                    actual_stream_size, boto_key_size, tries)
                continue
            if md5 != file["md5sum"]:
                self.logger.warning("actual md5sum (%s) does not match expected md5sum (%s)", md5, file["md5sum"])
                raise InvalidChecksumException()
            else:
                self.logger.info("actual md5sum (%s) matches expected md5sum (%s)", md5, file["md5sum"])
                return
        # if we get here, we couldn't get the file to be the correct
        # length, in five tries, so log a warning and mark it as
        # invalid
        self.logger.warning("Couldn't get a stream of the correct length in 5 tries!")
        raise InvalidChecksumException()

    def go(self):
        self.sanity_checks()
        # claim a file and upload it as a single session
        self.start_consul_session()
        self.start_time = int(time.time())
        try:
            with self.graph.session_scope():
                self.get_files_to_download()
                # we don't state transition here (just hold our locks)
                # because we want to transition one file at a time
                self.logger.info("Downloading analysis id %s from cghub", self.analysis_id)
                self.download()
                # first upload all files in the same session
                self.set_consul_state("uploading")
                for file in self.files:
                    with self.state_transition(file, "uploading", "uploaded"):
                        self.upload(file)
            # once they've been uploaded, verify checksums in a separate session
            with self.graph.session_scope():
                self.set_consul_state("validating")
                for file in self.files:
                    with self.state_transition(file, "validating", "live",
                                               error_states={InvalidChecksumException: "invalid"}):
                        self.verify(file)
            # note how long it took
            with self.graph.session_scope():
                now = int(time.time())
                took = now - self.start_time
                for file in self.files:
                    self.logger.info("Recording upload time, completed at %s, took %s seconds", now, took)
                    self.graph.node_update(file, system_annotations={"import_completed": now,
                                                                     "import_took": took})
        finally:
            self.cleanup()
