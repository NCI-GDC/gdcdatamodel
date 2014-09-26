import sys, os
import json
import re
from lxml import etree
from datetime import datetime, tzinfo, timedelta
from util  import xml2json
from io import BytesIO

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


class CGHubSignpostSync:

    def __init__(self):
        logger.debug('New CGHubSignpostSync')

    

    def update_doc(self, did, rev, doc):
        url = '/'.join([settings['signpost']['url'], did])
        req = requests.patch(url, 
                             params={ 'rev' : rev }, 
                             headers = {'content-type' : 'application/json'},
                             data = json.dumps(doc),
                             )

        if req.status_code != 201: raise ValueError(req.text)
    
        return req.json()

    def add_signpost(self, doc):
        did_r = requests.post(settings['signpost']['url'])
        did_doc = did_r.json()

        logger.debug('DID from signpost: %s' % did_doc)

        did = did_doc['did']
        rev = did_doc['rev']

        doc['did'] = did
        
        did_endpoint = '/'.join([settings['signpost']['url'], did])

        return self.update_doc(did, rev, doc)

    def in_signpost(self, analysis_id):
        find_endpoint = '/'.join([settings['signpost']['url'], 'find'])
        criteria = { 'analysis_id' : { '$' : analysis_id }}

        headers = {'content-type': 'application/json'}
        r = requests.get(find_endpoint, headers=headers, data=json.dumps(criteria))

        if r.status_code != 200: raise ValueError(r.text)

        dids = r.json()

        if len(dids['dids']) == 0:
            return False
        elif len(dids['dids']) == 1:
            return True
        else:
            logger.warning('Archive found twice in Signpost: %s' % archive_name)
        return True
        
                              
    def get_cghub_xml(self):
        url = ''.join([settings['cghub']['url'], settings['cghub']['query_str']])
        req = requests.get(url)

        if req.status_code != 200: raise ValueError(req.text)

        xml = str(req.text)
        tree = etree.fromstring(xml)
        convert = xml2json.xml2json()

        for element in tree.findall("Result"):
            convert.loadFromElement(element)
            doc_list = convert.toJSON()
            doc = doc_list[0]

            #check if exists already
            if self.in_signpost(doc['analysis_id']['$']):
                logger.debug('Analysis already in signpost: %s' % doc['analysis_id']['$'])
                continue

            doc['_type'] = 'cghub'

            #first setup url doc.
            url_doc = {}
            url_doc['urls'] = 'gtdownload://' + 'cghub.ucsc.edu/cghub/data/analysis/download/' + doc['analysis_id']['$']

            new_url_doc = self.add_signpost(url_doc)
            logger.debug('Added %s' % json.dumps(new_url_doc, indent=4))
            data_did = new_url_doc['did']
            doc['data_did'] = data_did

            meta_doc = { 'meta' : doc }
            new_meta_doc = self.add_signpost(meta_doc)
            logger.debug('Added %s' % json.dumps(new_meta_doc, indent=4))

            import_doc = {
                '_type' : 'cghub_import',
                'data_did' : data_did,
                'import' : {
                    'download' : {
                        'start_time' : None,
                        'finish_time' : None
                        },
                    'process' : {
                        'start_time' : None,
                        'finish_time' : None
                        },
                    'upload' : {
                        'start_time' : None,
                        'finish_time' : None
                        },
                    'host' : None,
                    'state' : 'not_started',
                    'start_time' : None,
                    'finish_time' : None
                    }
            }

            meta_import_doc = { 'meta' : import_doc }
            new_meta_import_doc = self.add_signpost(meta_import_doc)
            logger.debug('Added %s' % json.dumps(new_meta_import_doc, indent=4))

    def run(self):
        logger.info('TCGA CGHub Signpost Sync Running')
        self.get_cghub_xml()
