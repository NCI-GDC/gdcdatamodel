import os
import imp
import requests
import logging
import re
import tarfile
import urllib

from pprint import pprint
from py2neo import neo4j
from datetime import datetime, tzinfo, timedelta

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('PipelinePlugin', basePath)
logger     = logging.getLogger(name = "[{name}]".format(name = __name__))

#because python isoformat() isn't actually compliant without tz
class SimpleUTC(tzinfo):
    def tzname(self):
        return "UTC"
    def utcoffset(self, dt):
        return timedelta(0)


def md5sum(filename):
    md5 = hashlib.md5()
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(128 * md5.block_size), b''):
            md5.update(chunk)
    return md5.hexdigest()

def download_file(url, dl_dir="/tmp/tcga"):
    local_filename = os.path.join(dl_dir, url.split("/")[-1])
    r = requests.get(url, stream=True)
    if r.status_code != 200:
        return (r.text, r.status_code)

    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                f.flush()
    return (local_filename, r.status_code)

class PipelinePlugin(base.PipelinePluginBase):

    """
    extract_xml.py
    Opens a uri to tarfile and returns any extracted xml docs
    """

    def __iter__(self):
        for doc in self.docs:

            if doc['data_level'] != 'Level_1': continue
            if doc['platform'] != 'bio': continue

            url = doc['dcc_archive_url']
            stream = urllib.urlopen(url)
            tfile = tarfile.open(fileobj=stream, mode="r|gz")

            for xml in tfile:
                if not xml.name.endswith('.xml'): continue
                yield tfile.extractfile(xml).read()
            
            
