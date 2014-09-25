import sys, os
import json
import re
from datetime import datetime, tzinfo, timedelta

import requests
import logging, logging.config

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

#because python isoformat() isn't actually compliant without tz
class SimpleUTC(tzinfo):
    def tzname(self):
        return "UTC"
    def utcoffset(self, dt):
        return timedelta(0)


class TCGADCCSignpostSync:

    def __init__(self):
        logger.debug('New TCGADCCSignpostSync')

    def update_doc(self, did, rev, doc):
        url = '/'.join([settings['signpost']['url'], did])
        req = requests.patch(url, 
                             params={ 'rev' : rev }, 
                             headers = {'content-type' : 'application/json'},
                             data = json.dumps(doc),
                             )

        if req.status_code != 201: raise ValueError(req.text)
    
        return req.json()

    def pull_dcc_latest(self):
        r = requests.get(settings['tcga_dcc']['latest_url'])
        if r.status_code == 200:
            return r.text
        else:
            return None

    def parse_archive_name(self, archive):
        archive_name = archive['archive_name']
        pattern = re.compile(r"""
        ^
        (.+?)_ # center
        (\w+)\. # disease code
        (.+?)\. # platform
        (.+?\.)? # data level, ABI archives do not have data level part
        (\d+?)\. # batch
        (\d+?)\. # revision
        (\d+) # series
        $
        """, re.X)
        m = re.search(pattern, archive_name)
        archive['center_name'] = m.group(1)
        archive['disease_code'] = m.group(2)
        archive['platform'] = m.group(3)
        archive['data_level'] = m.group(4).replace('.', '') if m.group(4) else None
        archive['batch'] = int(m.group(5))
        archive['revision'] = int(m.group(6))


    def parse_archive_url(self, archive):
        archive_url = archive['dcc_archive_url']
        open_base_url = settings['tcga_dcc']['open_url']
        protected_base_url = settings['tcga_dcc']['protected_url']

        if (archive_url.startswith(open_base_url)): # open archive
            archive['protected'] = False
        elif (archive_url.startswith(protected_base_url)):# protected archive
            archive['protected'] = True
        else:
            logger.warning('Unmatched archive URL: ' + archive_url)

        logger.debug('archive_url: %s' % archive_url)
        parts = archive_url.split('/')

        if (parts[8].upper() != archive['disease_code']):
            logger.warning("Unmatched disease code between Archive URL and Archive Name: " + parts[8] + " vs " + archive['disease_code'])
        if (parts[10] != archive['center_name']):
            logger.warning("Unmatched center_name between Archive URL and Archive Name: " + parts[10] + " vs " + archive['center_name'])
        
        archive['center_type'] = parts[9]
        archive['platform_in_url'] = parts[11]
        archive['data_type_in_url'] = parts[12]

    
    def parse_archive(self, archive_name, date_added, archive_url):
        archive = {}
        archive['_type'] ='tcga_dcc_archive'
        archive['signpost_added'] = datetime.utcnow().replace(tzinfo=SimpleUTC()).replace(microsecond=0).isoformat()
        archive['import'] = {}
        archive['import']['state'] = 'not_started'
        archive['import']['host'] = None
        archive['import']['start_time'] = None
        archive['import']['finish_time'] = None
        archive['import']['download'] = {}
        archive['import']['download']['start_time'] = None
        archive['import']['download']['finish_time'] = None
        archive['import']['upload'] = {}
        archive['import']['upload']['start_time'] = None
        archive['import']['upload']['finish_time'] = None
        archive['import']['process'] = {}
        archive['import']['process']['start_time'] = None
        archive['import']['process']['finish_time'] = None
        
        archive['archive_name'] = archive_name
        archive['date_added'] = date_added
        archive['dcc_archive_url'] = archive_url

        self.parse_archive_name(archive)
        self.parse_archive_url(archive)
        return archive

    def in_signpost(self, archive_name):
        find_endpoint = '/'.join([settings['signpost']['url'], 'find'])
        type_filter = {}
        type_filter['_type'] = 'tcga_dcc_archive'
        type_filter['archive_name'] = archive_name
        headers = {'content-type': 'application/json'}
        
        r = requests.get(find_endpoint, headers=headers, data=json.dumps(type_filter))
        
        dids = r.json()
        if len(dids['dids']) == 0:
            return False
        elif len(dids['dids']) == 1:
            return True
        else:
            logger.warning('Archive found twice in Signpost: %s' % archive_name)
        return True

    def add_signpost(self, doc):
        did_r = requests.post(settings['signpost']['url'])
        did_doc = did_r.json()

        logger.debug('DID from signpost: %s' % did_doc)

        did = did_doc['did']
        rev = did_doc['rev']

        doc['did'] = did
        
        did_endpoint = '/'.join([settings['signpost']['url'], did])

        return self.update_doc(did, rev, doc)

                              
    def run(self):
        logger.info('TCGA DCC Signpost Sync Running')
        latest_report = self.pull_dcc_latest().splitlines()
        header = latest_report[0]
        if header != settings['tcga_dcc']['latest_firstline']:
            logger.error('Unexpected header for latest report: "%s"' % header)
            sys.exit(-1)

        for line in latest_report[1:]:
            sline = line.strip().split('\t')
            archive_name = sline[0]
            date_added = sline[1]
            archive_url = sline[2]

            if not self.in_signpost(archive_name):
                url_doc = {}
                url_doc['urls'] = [archive_url]
                sp_url_doc = self.add_signpost(url_doc)
                data_did = sp_url_doc['did']
                
                date_added = datetime.strptime(sline[1], '%m/%d/%Y %H:%M')
                date_added = date_added.replace(tzinfo=SimpleUTC()).isoformat()
                meta_doc = {}
                meta_doc['meta'] = self.parse_archive(archive_name, date_added, archive_url)
                meta_doc['meta']['data_did'] = data_did
                logger.info('Adding archive %s' % archive_name)
                logger.debug('Doc: %s' % json.dumps(meta_doc, sort_keys=True, indent=4))
                self.add_signpost(meta_doc)
            
            else:
                logger.debug('Archive already in Signpost %s' % archive_name)
