import os
import json
import requests
import logging
import tarfile
import hashlib
import subprocess
import dateutil.parser
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

class TCGADCCImportReporter:

    def __init__(self):
        logger.debug('New instance of %s' % __name__)

    def update_doc(self, did, rev, doc):
        url = '/'.join([settings['signpost']['url'], did])
        req = requests.patch(url,
                             params={ 'rev' : rev },
                             headers = {'content-type' : 'application/json'},
                             data = json.dumps(doc),
                             )

        if req.status_code != 201: raise ValueError(req.text)

        return req.json()

    def submit_report(self, report):
        did_r = requests.post(settings['signpost']['url'])
        
        if did_r.status_code != 201: raise ValueError(did_r.text)

        did_doc = did_r.json()
        did = did_doc['did']
        rev = did_doc['rev']
        did_doc['meta'] = report

        doc = self.update_doc(did, rev, did_doc)
        logger.info("Submitted Report %s" % doc['did'])

    def generate_report(self):
        report = {'_type' : 'tcga_dcc_report', 'timestamp' : timestamp()}
        criteria = { '_type' : 'tcga_dcc_archive'}
        headers = {'content-type': 'application/json'}
        find_url = '/'.join([settings['signpost']['url'], 'find'])

        req = requests.get(find_url,headers=headers, data=json.dumps(criteria))

        if req.status_code != 200: raise ValueError(req.text)

        all_dids = req.json()['dids']

        for did in all_dids:
            get_url = '/'.join([settings['signpost']['url'], did])
            req = requests.get(get_url)
            if req.status_code != 200: raise ValueError(req.text)
            doc = req.json()
            study = doc['meta']['disease_code']
            platform = doc['meta']['platform']
            
            if study not in report:
                report[study] = {}

            if platform not in report[study]:
                report[study][platform] = {
                    'total_archives' : 0, 
                    'not_started' : 0,
                    'in_progress' : 0,
                    'completed' : 0,
                    'imported_archives' : []
                }

            report[study][platform]['total_archives'] = report[study][platform]['total_archives'] + 1

            if doc['meta']['import']['state'] == 'complete':
                archive_name = doc['meta']['archive_name']
                archive_size = doc['meta']['archive_filesize']
                download_start = dateutil.parser.parse(doc['meta']['import']['download']['start_time'])
                download_finish = dateutil.parser.parse(doc['meta']['import']['download']['finish_time'])
                upload_start = dateutil.parser.parse(doc['meta']['import']['upload']['start_time'])
                upload_finish = dateutil.parser.parse(doc['meta']['import']['upload']['finish_time'])

                download_time = (download_finish - download_start).total_seconds()

                if download_time == 0:
                    download_time = 1

                download_speed = float(archive_size) / float(download_time)

                upload_time = (upload_finish - upload_start).total_seconds()
                if upload_time == 0:
                    upload_time = 1

                upload_speed = float(archive_size) / float(upload_time)

                archive_import = (archive_name, archive_size, download_speed, upload_speed)
                
                report[study][platform]['imported_archives'].append(archive_import)
                report[study][platform]['completed'] = report[study][platform]['completed'] + 1

            elif doc['meta']['import']['state'] == 'not_started':
                report[study][platform]['not_started'] = report[study][platform]['not_started'] + 1
            else:
                report[study][platform]['in_progress'] = report[study][platform]['in_progress'] + 1

            
        return report

    def run(self):
        logger.info("Starting run")
        report = self.generate_report()
        self.submit_report(report)
