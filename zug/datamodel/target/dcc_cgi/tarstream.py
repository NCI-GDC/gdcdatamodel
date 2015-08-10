import os, sys
import tarfile
import time
import md5
from cStringIO import StringIO as BIO
import requests
import urllib3
import logging
import md5

urllib3.disable_warnings()
logging.captureWarnings(True)

class Stream(object):
    #def __init__(self, url, name, auth_data, chunk_size=1073741824, expected_md5_sum=None, size=None, calc_md5=False):
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

    @property
    def filename(self):
        return self.name

    def __iter__(self):
        for chunk in self.iterable:
            self._bytes_streamed += len(chunk)
            if self.calc_md5:
                self.md5_val.update(chunk)
            #sys.stdout.write("%d bytes\r" % len(chunk))
            #sys.stdout.flush()
            yield chunk

    def get_md5(self):
        result = "NULL"
        if self.calc_md5:
            result = self.md5_val.hexdigest()

        return result

    def connect(self):
        #os.environ['http_proxy'] = "http://cloud-proxy:3128"
        #os.environ['https_proxy'] = "http://cloud-proxy:3128"
        print "Connecting to", self.url         
        try:
            self.result = requests.get(self.url, auth=(self.auth_data['id'], self.auth_data['pw']), stream=True, verify=False)
        except:
            print "Error on request:", sys.exc_info()[1]
        else:
            if self.result.status_code == 200:
                self.size = int(self.result.headers['content-length'])
                #print "Connected to %s, length = %d" % (self.url, self.size)
                self.iterable = self.result.iter_content(self.chunk_size)
            else:
                print "Connect to %s failed: %s %s" % (self.url, self.result.status_code, self.result.reason)
        #del os.environ['https_proxy']
        #del os.environ['http_proxy']

class TarStream(object):
    #def __init__(self, streams, file_name, manifest=False, md5_sum=True, chunk_size=134217728):
    def __init__(self, streams, file_name, manifest=False, md5_sum=True, chunk_size=131072):
        self.streams = streams
        self.buffer = ""
        self.chunk_size = chunk_size
        self.tar_file = tarfile.open(name=file_name, mode='w|gz', fileobj=self)
        if md5_sum:
            self.md5_sum = md5.new()
        self.manifest = manifest
        #self.out_file = open("tmp_" + file_name, 'wb')

    def write(self, data):
        #self.md5_sum.update(data)
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
        #self.out_file.write(data)
        self.tar_file.offset += len(data)
        for chunk in self._yield_from_buffer():
            yield chunk

    #def write_tar_data_to_file(self):    


    def __iter__(self):
        if self.manifest:
            print "No manifest created yet"
            # create a manifest here

        for stream in self.streams:
            print "Tar - processing stream %s" % stream.name
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
        #else:
        print "Tarfile complete, closing"
        self.tar_file.close()
        #self.out_file.close()
        for chunk in self._yield_from_buffer(flush=True):
            yield chunk


