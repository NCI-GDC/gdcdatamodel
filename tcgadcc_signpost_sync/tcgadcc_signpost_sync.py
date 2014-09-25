import sys
import json
import re
from datetime import datetime, tzinfo, timedelta

import requests
import logging

TCGA_DCC_LATEST = 'http://tcga-data.nci.nih.gov/datareports/resources/latestarchive'
LATEST_FIRSTLINE = 'ARCHIVE_NAME\tDATE_ADDED\tARCHIVE_URL'
SIGNPOST_URL = 'http://172.16.128.94'

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s %(name)-6s %(levelname)-4s %(message)s',
)

#because python isoformat() isn't actually compliant without tz
class SimpleUTC(tzinfo):
    def tzname(self):
        return "UTC"
    def utcoffset(self, dt):
        return timedelta(0)


def pull_dcc_latest():
    r = requests.get(TCGA_DCC_LATEST)
    if r.status_code == 200:
        return r.text
    else:
        return None

def parse_archive_name(archive):
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


def parse_archive_url(archive):
    archive_url = archive['archive_url']
    open_base_url = 'https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/'
    protected_base_url = 'https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/tcga4yeo/tumor/'

    if (archive_url.startswith(open_base_url)): # open archive
        archive['protected'] = False
    elif (archive_url.startswith(protected_base_url)):# protected archive
        archive['protected'] = True
    else:
        logging.warning('Unmatched archive URL: ' + archive_url)

    logging.debug('archive_url: %s' % archive_url)
    parts = archive_url.split('/')

    if (parts[8].upper() != archive['disease_code']):
        logging.warning("Unmatched disease code between Archive URL and Archive Name: " + parts[8] + " vs " + archive['disease_code'])
    if (parts[10] != archive['center_name']):
        logging.warning("Unmatched center_name between Archive URL and Archive Name: " + parts[10] + " vs " + archive['center_name'])
        
    archive['center_type'] = parts[9]
    archive['platform_in_url'] = parts[11]
    archive['data_type_in_url'] = parts[12]

    
def parse_archive(archive_name, date_added, archive_url):
    archive = {}
    archive['_type'] ='tcga_dcc_archive'
    archive['signpost_added'] = datetime.utcnow().replace(tzinfo=SimpleUTC()).replace(microsecond=0).isoformat()
    archive['import'] = {}
    archive['import']['state'] = 'not_started'
    archive['import']['host'] = None
    archive['import']['start_time'] = None
    archive['import']['finish_time'] = None

    archive['archive_name'] = archive_name
    archive['date_added'] = date_added
    archive['archive_url'] = archive_url

    parse_archive_name(archive)
    parse_archive_url(archive)
    return archive

def in_signpost(archive_name):
    find_endpoint = '/'.join([SIGNPOST_URL, 'find'])
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
        logging.warning('Archive found twice in Signpost: %s' % archive_name)
        return True

def add_signpost(doc):
    did_r = requests.post(SIGNPOST_URL)
    did_doc = did_r.json()

    logging.debug('DID from signpost: %s' % did_doc)

    did = did_doc['did']
    rev = did_doc['rev']

    doc['did'] = did

    did_endpoint = '/'.join([SIGNPOST_URL, did])
    

    update_r = requests.patch(did_endpoint, 
                              params={ 'rev' : rev }, 
                              headers = {'content-type' : 'application/json'},
                              data = json.dumps(doc),
                              )

    if update_r.status_code != 201: raise ValueError(update_r.text)
    
    return update_r.json()
                              
def main():
    latest_report = pull_dcc_latest().splitlines()
    header = latest_report[0]
    if header != LATEST_FIRSTLINE:
        logging.error('Unexpected header for latest report: "%s"' % header)
        sys.exit(-1)

    for line in latest_report[1:]:
        sline = line.strip().split('\t')
        archive_name = sline[0]
        date_added = sline[1]
        archive_url = sline[2]

        if not in_signpost(archive_name):
            url_doc = {}
            url_doc['urls'] = archive_url
            sp_url_doc = add_signpost(url_doc)
            data_did = sp_url_doc['did']

            date_added = datetime.strptime(sline[1], '%m/%d/%Y %H:%M')
            date_added = date_added.replace(tzinfo=SimpleUTC()).isoformat()
            meta_doc = {}
            meta_doc['meta'] = parse_archive(archive_name, date_added, archive_url)
            meta_doc['meta']['data_did'] = data_did
            logging.info("Adding archive: %s" % json.dumps(meta_doc, sort_keys=True, indent=4))
            add_signpost(meta_doc)

            
        else:
            logging.info('Archive already in Signpost %s' % archive_name)

if __name__ == '__main__':
    main()

