import os
import json
import requests
import logging
import tarfile
import hashlib
import subprocess
from datetime import datetime, tzinfo, timedelta

from app import settings

#TODO standardize where this happenes
log_format = settings['logging'].get('format', '%(asctime)s %(name)-6s %(levelname)-4s %(message)s')
logger = logging.getLogger(__name__)
if 'log_dir' not in settings['logging']:
    logging.basicConfig(format=log_format)
else:
    log_file = os.path.join(settings['logging']['log_dir'], __name__ + '.log')
    fh = logging.FileHandler(log_file)
    formatter = logging.Formatter(log_format)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

logger.setLevel(settings['logging'].get('level', logging.INFO))

#TODO time utilities should go somewhere else
class SimpleUTC(tzinfo):
    def tzname(self):
        return "UTC"
    def utcoffset(self, dt):
        return timedelta(0)

def timestamp():
    return datetime.utcnow().replace(tzinfo=SimpleUTC()).replace(microsecond=0).isoformat()

class TCGADCCDataDownloader:

    def __init__(self):
        logger.debug('New instance of %s' % __name__)

    def _get_first_did(self,criteria):
        find_endpoint = '/'.join([settings['signpost']['url'], 'find'])
        headers = {'content-type': 'application/json'}
        r = requests.get(find_endpoint, headers=headers, data=json.dumps(criteria))
        if r.status_code != 200: raise ValueError(r.text)
        dids = r.json()

        if len(dids['dids']) == 0:
            return None
        else:
            return dids['dids'][0]


    def _download_file(self, url, dl_dir):
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

    def _get_file_list(self,filename):
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

    #TODO this should be a call to signpostclient library
    def update_doc(self, did, rev, doc):
        url = '/'.join([settings['signpost']['url'], did])
        req = requests.patch(url,
                             params={ 'rev' : rev },
                             headers = {'content-type' : 'application/json'},
                             data = json.dumps(doc),
                         )
        #OK with conflicts - typically some other host claimed the work before us
        #if req.status_code != 201: raise ValueError(req.text)
        logger.debug('status_code: %s resp: %s' % (req.status_code, json.dumps(req.json(), indent=4)))
        return (req.status_code, req.json())

    def update_url_doc(self, data_did, swift_url):
        url = '/'.join([settings['signpost']['url'], data_did])
        req = requests.get(url)
        
        if req.status_code != 200: raise ValueError(req.text)

        doc = req.json()
        logger.debug("url doc: %s" % json.dumps(doc, indent=4))
        did = doc['did']
        rev = doc['rev']
        doc['urls'].insert(0, swift_url)

        logger.debug("Updating URL doc %s %s %s " % (did, rev, doc))

        (status_code, resp) = self.update_doc(did, rev, doc)
        
        if status_code != 201: raise ValueError(resp)

        return (status_code, resp)

    def md5hash(self, filename):
        md5 = hashlib.md5()
        with open(filename, 'rb') as f:
            for chunk in iter(lambda: f.read(128 * md5.block_size), b''):
                md5.update(chunk)
        return md5.hexdigest()

    def resume_work(self):
        criteria = { 
            '_type' : 'tcga_dcc_archive',
            'import' : { 'host' : settings['download']['id'] },
        }


        did = self._get_first_did(criteria)

        if did is None:
            return None

        url = '/'.join([settings['signpost']['url'], did])
        r = requests.get(url)
        if r.status_code != 200: raise ValueError(r.text)
        return r.json()

    def get_work(self,protected=False):
        criteria = { '_type' : 'tcga_dcc_archive',
                     'import' : { 'state' : 'not_started' },
                     'protected' : protected
                   }

        return self._get_first_did(criteria)

    def claim_work(self,did):
        url = '/'.join([settings['signpost']['url'], did])
        r = requests.get(url)

        if r.status_code != 200: raise ValueError(r.text)

        doc = r.json()
        rev = doc['rev']

        doc['meta']['import']['host'] = settings['download']['id']
        doc['meta']['import']['state'] = 'downloading'
        doc['meta']['import']['start_time'] = timestamp()

        doc['meta']['import']['download']['start_time'] = timestamp()
        doc['meta']['import']['download']['finish_time'] = None

        (update_status_code, doc) = self.update_doc(did, rev, doc)

        if update_status_code != 201:
            return None
        
        return doc

    def do_work(self, doc):
        did = doc['did']
        rev = doc['rev']
        url = '/'.join([settings['signpost']['url'], doc['did']])

        archive_url = doc['meta']['dcc_archive_url']

        logger.info('Downloading: %s' % archive_url)

        (filename, status_code) = self._download_file(archive_url, settings['download']['dir'])

        if status_code != 200: raise ValueError('Unexpected download status: %s' % status_code)

        doc['meta']['archive_filesize'] = os.path.getsize(filename)
        doc['meta']['import']['download']['finish_time'] = timestamp()
        doc['meta']['import']['state'] = 'processing'
        doc['meta']['import']['process']['start_time'] = timestamp()

        (status_code, doc) = self.update_doc(did, rev, doc)
        if status_code != 201: raise ValueError(doc)
        rev = doc['rev']

        file_list = self._get_file_list(filename)
        md5 = self.md5hash(filename)
        doc['meta']['archive_file_list'] = file_list
        doc['meta']['archive_md5'] = md5
        doc['meta']['import']['process']['finish_time'] = timestamp()
        doc['meta']['import']['state'] = 'uploading'

        doc['meta']['import']['upload']['start_time'] = timestamp()
        doc['meta']['import']['upload']['finish_time'] = None

        (status_code, doc) = self.update_doc(did, rev, doc)
        if status_code != 201: raise ValueError(doc)
        rev = doc['rev']

        segment_size = "1000000000"
        if doc['meta']['protected']:
            container = "tcga_dcc_protected"
        else:
            container = "tcga_dcc_public"

        object_name = '/'.join([doc['meta']['disease_code'], did, os.path.basename(filename)])

        if doc['meta']['archive_filesize'] > segment_size:
            swift_cmd = ['swift', 'upload', '--segment-size', segment_size, '--object-name', object_name, container, filename]
        else:
            swift_cmd = ['swift', 'upload', '--object-name', object_name, container, filename]

        logger.info("swift_cmd: %s" % swift_cmd)
        upload_proc = subprocess.Popen(swift_cmd)
        stdout, stderr = upload_proc.communicate()
        rc = upload_proc.returncode

        if rc != 0:
            doc['meta']['import']['state'] = 'error'
            doc['meta']['import']['message'] = "swift upload failed: %s %s" % (stdout, stderr)
        else:
            doc['meta']['import']['state'] = 'complete'
            doc['meta']['import']['upload']['finish_time'] = timestamp()
            doc['meta']['import']['finish_time'] = timestamp()
            doc['meta']['import']['host'] = None
            
            data_did = doc['meta']['data_did']
            swift_url = 'swift://' + container + '/' + object_name
            (url_status_code, url_resp) = self.update_url_doc(data_did, swift_url)
            os.remove(filename)
            
        (status_code, doc) = self.update_doc(did, rev, doc)

        if status_code != 201: raise ValueError(doc)
        return doc


    def run(self):
        logger.info("Starting run")
        work = self.resume_work()
        if work is None:
            dl_did = self.get_work()
            if dl_did is None:
                logger.info('No work')
                return
            work = self.claim_work(dl_did)
        else:
            logger.info('Resuming work: %s' % work['did'])

        if work is not None:
            logger.info('Starting work: %s' % work['did'])
            doc = self.do_work(work)
            logger.info('Completed work: %s ' % work['did'])
        else:
            logger.info('Could not claim work')


