import os, sys
import tarfile
import time
import md5
from cStringIO import StringIO as BIO
import requests
import urllib3
import logging
import md5
from cdisutils.log import get_logger

urllib3.disable_warnings()
logging.captureWarnings(True)

class Stream(object):
    def __init__(self, url, name, auth_data, chunk_size=1000000, expected_md5_sum=None, size=None, calc_md5=False):
        self.name = name
        self.url = url
        self.iterable = None
        self.expected_md5_sum = expected_md5_sum
        self.calc_md5_sum = None
        self.calc_md5 = calc_md5
        if self.calc_md5:
            self.md5_val = md5.new()
        self.size = size
        self.chunk_size = chunk_size
        self.auth_data = auth_data
        self._bytes_streamed = 0
        self.result = None
        self.log = get_logger("target_dcc_cgi_stream_" + str(os.getpid()))

    @property
    def filename(self):
        return self.name

    def __iter__(self):
        for chunk in self.iterable:
            self._bytes_streamed += len(chunk)
            if self.calc_md5:
                self.md5_val.update(chunk)
            yield chunk

    def get_md5(self):
        result = "NULL"
        if self.calc_md5:
            result = self.md5_val.hexdigest()

        return result

    def connect(self):
        self.log.info("Connecting to %s", self.url)         
        try:
            self.result = requests.get(self.url, auth=(self.auth_data['id'], self.auth_data['pw']), stream=True, verify=False)
        except:
            error_str = "Error on request:", sys.exc_info()[1]
            self.log.error(error_str)
            raise RuntimeError(error_str)
        else:
            if self.result.status_code == 200:
                self.size = int(self.result.headers['content-length'])
                self.iterable = self.result.iter_content(self.chunk_size)
            else:
                error_str = "Connect to %s failed: %s %s" % (self.url, self.result.status_code, self.result.reason)
                self.log.error(error_str)
                raise RuntimeError(error_str)

class TarStream(object):
    def __init__(self, streams, file_name, manifest=False, md5_sum=True, chunk_size=131072):
        self.streams = streams
        self.buffer = ""
        self.chunk_size = chunk_size
        self.tar_file = tarfile.open(name=file_name, mode='w|gz', fileobj=self)
        if md5_sum:
            self.md5_sum = md5.new()
        self.manifest = manifest
        self.log = get_logger("target_dcc_cgi_tarstream_" + str(os.getpid()))

    def write(self, data):
        self.buffer += data

    def flush(self):
        pass

    def _yield_from_buffer(self, flush=False):
        while len(self.buffer) >= self.chunk_size:
            yield self.buffer[:self.chunk_size]
            self.buffer = self.buffer[self.chunk_size:]
        if flush:
            yield self.buffer
            self.buffer = ""

    def _write_and_yield(self, data):
        self.tar_file.fileobj.write(data)
        self.tar_file.offset += len(data)
        for chunk in self._yield_from_buffer():
            yield chunk

    def __iter__(self):
        if self.manifest:
            self.log.info("No manifest created yet")
            # TODO: create a manifest here

        for stream in self.streams:
            self.log.info("Tar - processing stream %s" % stream.name)
            stream.connect()
            tinfo = tarfile.TarInfo(name=stream.name)
            if stream.size != None:
                tinfo.size = int(stream.size)
            tinfo.mtime = time.time()
            tinfo.type = tarfile.REGTYPE
            infobuf = tinfo.tobuf(
                self.tar_file.format,
                self.tar_file.encoding,
                self.tar_file.errors)
            for chunk in self._write_and_yield(infobuf):
                yield chunk
            for chunk in stream:
                for subchunk in self._write_and_yield(chunk):
                    yield subchunk

            # fill the remaining space in block
            _, remainder = divmod(tinfo.size, tarfile.BLOCKSIZE)
            if remainder > 0:
                for chunk in self._write_and_yield(tarfile.NUL * (tarfile.BLOCKSIZE - remainder)):
                    yield chunk

        self.log.info("Tarfile complete, closing")
        self.tar_file.close()
        for chunk in self._yield_from_buffer(flush=True):
            yield chunk


