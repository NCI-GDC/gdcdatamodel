import os
import imp
import requests
import logging
import re
import py2neo

from pprint import pprint
from py2neo import neo4j
from datetime import datetime, tzinfo, timedelta

from zug import basePlugin
from zug.exceptions import IgnoreDocumentException

logger = logging.getLogger(name = "[{name}]".format(name = __name__))

#because python isoformat() isn't actually compliant without tz
class SimpleUTC(tzinfo):
    def tzname(self):
        return "UTC"
    def utcoffset(self, dt):
        return timedelta(0)

class latest_urls(basePlugin):

    """
    
    """
        
    def initialize(self, constraints = {}, **kwargs):
        self.open_url = kwargs['open_url']
        self.latest_firstline = kwargs['latest_firstline']
        self.latest_url = kwargs['latest_url']
        self.protected_url = kwargs['protected_url']
        
    def start(self):
        self.docs = []
        logger.info('TCGA DCC Signpost Sync Running')
        latest_report = self.pull_dcc_latest().splitlines()
        header = latest_report[0]
 
        if header != self.latest_firstline:
            logger.error('Unexpected header for latest report: "%s"' % header)
            sys.exit(-1)
    
        for line in latest_report[1:]:
            sline = line.strip().split('\t')
            self.docs.append(sline)
 
    def process(self, doc):

        archive = self.parse_archive(*doc)
        for key, value in self.kwargs.get('constraints', {}).iteritems():
            if archive[key] != value: 
                raise IgnoreDocumentException()
        
        url = archive[self.kwargs['url_key']]
        self.state['archive'] = archive

        return url

    def pull_dcc_latest(self):
        r = requests.get(self.latest_url)
        if r.status_code != 200: 
            return None
        return r.text

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
        open_base_url = self.open_url
        protected_base_url = self.protected_url

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
