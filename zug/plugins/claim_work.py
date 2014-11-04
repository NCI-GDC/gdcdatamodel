import os
import imp
import requests
import logging
import yaml
import copy
import traceback
import uuid
import time 
import json
import random
import atexit 
import sys
import subprocess
import hashlib
import re

from os import listdir
from os.path import isfile, join
from pprint import pprint
from zug import basePlugin
from zug.exceptions import IgnoreDocumentException, EndOfQueue

currentDir = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(name = "[{name}]".format(name = __name__))

class NoMoreWork(Exception):
    pass

class claim_work(basePlugin):

    """
    takes in an xml as a string and compiles a list of nodes and edges
    """
    
    def initialize(self, **kwargs):
        atexit.register(self.check_error)

        assert 'cghub_key' in kwargs, 'Please specify path to a cghub downloader key: cghub_key'
        assert 'download_path' in kwargs, 'Please specify directory to place the file: download_path'

        self.signpost = 'http://{signpost}/v0/'.format(signpost=kwargs.get('signpost', 'signpost'))

        self.check_count = int(kwargs.get('check_count', 5))
        # self.id = str(uuid.uuid4())
        with open('/etc/tungsten/name') as f:
            self.id = f.read().strip()

        self.state = 'IDLE'
        self.work = None
        self.bai = None
        self.url = 'http://{host}:{port}/db/data/cypher'.format(
            host=kwargs.get('neo4j', 'neo4j'), port=kwargs.get('port', '7474'))

    def set_state(self, state):
        self.state = state
        logger.info("Entering state: {0}".format(state))

    def start(self):
        while True:
            self.do_carefully(self.get_work)
            self.do_carefully(self.download)
            self.do_carefully(self.checksum)
            self.do_carefully(self.upload)
            self.do_carefully(self.post)
            self.do_carefully(self.finish_work)
            self.check_error()

    def do_carefully(self, func):
        try: 
            func()
        except KeyboardInterrupt: 
            self.post_error('KeyboardInterrupt: Process was stopped by user')
            raise
        except NoMoreWork: 
            self.check_error()
        except Exception, msg:
            traceback.print_exc()
            logging.error('Downloader errored while executing {f}'.format(f=func))
            self.check_error(msg)
            return self.start()

    def verify_claim(self, file_id):
        time.sleep(random.random()*10 + 1)
        result = self.submit([
            'MATCH (n:file {{id:"{file_id}"}})',
            'WHERE n.importer="{id}"',
            'RETURN n', 
        ], file_id=file_id, id=self.id)

        if len(result['data']) == 0: 
            return False
        return True

    def claim_work(self):
        result = self.submit([
            'MATCH (n:file)',
            'WHERE n.import_state="NOT_STARTED"'
            'AND right(n.file_name, 4) <> ".bai"',
            # 'OR n.import_state="ERROR"',
            # 'AND right(n.file_name, 4) <> ".bai"',
            'WITH n LIMIT 1',
            'RETURN n',
            ])

        if not len(result['data']):
            self.set_state('EXITING')
            raise NoMoreWork('No More Work')

        file_id = result['data'][0][0]['data']['id']

        result = self.submit([
            'MATCH (n:file {{id:"{file_id}"}})',
            'WHERE n.import_state="NOT_STARTED"'
            # 'OR n.import_state="ERROR"',
            'SET n.importer="{id}", n.import_state="STARTED"',
            'RETURN n', 
        ], file_id=file_id, id=self.id)

        try: self.work = result['data'][0][0]['data']
        except: return False
        
        for i in range(self.check_count):
            if not self.verify_claim(file_id): 
                return False                
                
        self.set_state('SCHEDULED')
        logger.info("Claimed: {0}".format(self.work['id']))
        return True        

    def post_error(self, msg = "none"):
        msg = str(msg)
        logger.warn("Posting error state: " + msg)
        try:
            if not self.work: 
                logger.error("No work to set to state ERROR.")
                return
            result = self.submit([
                'MATCH (n:file {{id:"{file_id}"}})',
                'SET n.import_state="ERROR", n.error_msg="{msg}"',
            ], file_id=self.work['id'], msg=msg)
            logger.warn("Successfully set file state to ERROR.")
        except Exception, msg: 
            logger.error("UNABLE TO POST ERRORED STATE TO DATAMODEL !!")
            logger.error(str(msg))
    

    def check_error(self, msg = 'none'):
        logger.info("Checking for correct exit state, state = {0}".format(self.state))
        should_be_exiting = ['IDLE', 'EXITING']

        if self.state in should_be_exiting:
            logger.info("state okay.")
            return 
        logger.error("DOWNLOADER EXITING WITH ERRORED STATE.")
        self.post_error(msg)

        for f in self.files:
            try: self.delete_scratch(f)
            except: logger.error("Unable to delete scratch.  Will likely run out of space in the future")

        self.work = None
