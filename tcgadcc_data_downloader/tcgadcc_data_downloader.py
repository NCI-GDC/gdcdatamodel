import os
import json
import requests
import logging
import tarfile
import hashlib
import subprocess
from datetime import datetime, tzinfo, timedelta

SIGNPOST_URL = 'http://172.16.128.94'
HOST = 'dcc_data_downloader_0'
LOCAL_DIR = '/mnt/cinder/download'

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s %(name)-6s %(levelname)-4s %(message)s',
)

class SimpleUTC(tzinfo):
    def tzname(self):
        return "UTC"
    def utcoffset(self, dt):
        return timedelta(0)


def _get_first_did(criteria):
    find_endpoint = '/'.join([SIGNPOST_URL, 'find'])
    headers = {'content-type': 'application/json'}
    r = requests.get(find_endpoint, headers=headers, data=json.dumps(criteria))
    if r.status_code != 200: raise ValueError(r.text)
    dids = r.json()

    if len(dids['dids']) == 0:
        return None
    else:
        return dids['dids'][0]


def _download_file(url, dl_dir=LOCAL_DIR):
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

def _get_file_list(filename):
    tar = tarfile.open(filename)
    file_list = []
    for member in tar.getmembers():
        f = {}
        f['filename'] = member.name
        f['size'] = member.size
        f['mtime'] = member.mtime
        file_list.append(f)

    tar.close()
    return file_list
    

def timestamp():
    return datetime.utcnow().replace(tzinfo=SimpleUTC()).replace(microsecond=0).isoformat()

def md5hash(filename):
    md5 = hashlib.md5()
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(128 * md5.block_size), b''):
            md5.update(chunk)
    return md5.hexdigest()

def resume_work():
    criteria = { '_type' : 'tcga_dcc_archive',
                 'import' : { 'host' : HOST },
               }

    did = _get_first_did(criteria)

    if did is None:
        return None

    url = '/'.join([SIGNPOST_URL, did])
    r = requests.get(url)
    if r.status_code != 200: raise ValueError(r.text)
    return r.json()

def get_work(protected=False):
    criteria = { '_type' : 'tcga_dcc_archive',
                 'import' : { 'host' : None },
                 'protected' : protected
               }

    return _get_first_did(criteria)

def claim_work(did):
    url = '/'.join([SIGNPOST_URL, did])
    r = requests.get(url)

    if r.status_code != 200: raise ValueError(r.text)

    doc = r.json()

    logging.info(json.dumps(doc, sort_keys=True, indent=4))

    doc['meta']['import']['host'] = HOST
    doc['meta']['import']['state'] = 'downloading'
    doc['meta']['import']['start_time'] = timestamp()

    doc['meta']['download'] = {}
    doc['meta']['download']['start_time'] = timestamp()
    doc['meta']['download']['finish_time'] = None

    update_r = requests.patch(url,
                             params={ 'rev' : doc['rev']},
                             headers = {'content-type' : 'application/json'},
                             data = json.dumps(doc)
                             )

    #can have conflict if other host gets there first, just continue to get work
    if update_r.status_code != 201:
        return None

    return update_r.json()

def do_work(doc):
    url = '/'.join([SIGNPOST_URL, doc['did']])

    archive_url = doc['meta']['archive_url']

    logging.info('Downloading: %s' % archive_url)

    (filename, status_code) = _download_file(archive_url)

    if status_code != 200: raise ValueError('Unexpected download status: %s' % status_code)

    doc['meta']['archive_filesize'] = os.path.getsize(filename)
    doc['meta']['download']['finish_time'] = timestamp()
    doc['meta']['import']['state'] = 'processing'

    update_r = requests.patch(url,
                             params={ 'rev' : doc['rev']},
                             headers = {'content-type' : 'application/json'},
                             data = json.dumps(doc)
                             )

    if update_r.status_code != 201: raise ValueError(update_r.text)
    doc = update_r.json()

    file_list = _get_file_list(filename)
    md5 = md5hash(filename)
    doc['meta']['archive_file_list'] = file_list
    doc['meta']['archive_md5'] = md5
    doc['meta']['import']['state'] = 'uploading'

    doc['meta']['upload'] = {}
    doc['meta']['upload']['start_time'] = timestamp()
    doc['meta']['upload']['finish_time'] = None

    update_r = requests.patch(url,
                              params={ 'rev' : doc['rev']},
                              headers = {'content-type' : 'application/json'},
                              data = json.dumps(doc)
                             )

    if update_r.status_code != 201: raise ValueError(update_r.text)
    doc = update_r.json()


    segment_size = "1000000000"
    if doc['meta']['protected']:
        container = "tcga_dcc_protected"
    else:
        container = "tcga_dcc_public"

    if doc['meta']['archive_filesize'] > segment_size:
        swift_cmd = ['swift', 'upload', '--segment-size', segment_size, '--object-name', os.path.basename(filename), container, filename]
    else:
        swift_cmd = ['swift', 'upload', '--object-name', os.path.basename(filename), container, filename]

    upload_proc = subprocess.Popen(swift_cmd)
    stdout, stderr = upload_proc.communicate()
    rc = upload_proc.returncode

    if rc != 0:
        doc['meta']['import']['state'] = 'error'
        doc['meta']['import']['message'] = "swift upload failed: %s" % (stderr)
    else:
        doc['meta']['import']['state'] = 'complete'
        doc['meta']['import']['finish_time'] = timestamp()

    update_r = requests.patch(url,
                              params={ 'rev' : doc['rev']},
                              headers = {'content-type' : 'application/json'},
                              data = json.dumps(doc)
                             )

    if update_r.status_code != 201: raise ValueError(update_r.text)
    return update_r.json()

    
def main():
    while True:

        work = resume_work()
        if work is None:
            dl_did = get_work()
            if dl_did is None:
                return
        
            work = claim_work(dl_did)

        logging.info('work: %s' % json.dumps(work, sort_keys=True, indent=4))

        if work is not None:
            doc = do_work(work)
            logging.info('Completed work: %s ' % json.dumps(doc, sort_keys=True, indent=4))
        

    
if __name__ == '__main__':
    main()
