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

class download_consumer(basePlugin):

    """
    takes in an xml as a string and compiles a list of nodes and edges
    """
    
    def initialize(self, **kwargs):
        atexit.register(self.check_error)

        assert 'cghub_key' in kwargs, 'Please specify path to a cghub downloader key: cghub_key'
        assert 'download_path' in kwargs, 'Please specify directory to place the file: download_path'

        self.check_count = int(kwargs.get('check_count', 5))
        self.id = str(uuid.uuid4())

        self.state = 'IDLE'
        self.work = None

        self.url = 'http://{host}:{port}/db/data/cypher'.format(
            host=kwargs.get('host', 'localhost'), port=kwargs.get('port', '7474'))

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
            self.post_error('KeyboardInterrupt')
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
        return True        

    def finish_work(self):
        self.set_state('FINISHING')
        result = self.submit([
            'MATCH (n:file {{id:"{file_id}"}})',
            'SET n.import_state="COMPLETE"',
            'REMOVE n.importer',
        ], file_id=self.work['id'])
        self.set_state('IDLE')

    def download(self):
        self.set_state('DOWNLOADING')
        if self.work is None: raise Exception('download() was called with no work')
        directory = self.kwargs['download_path']

        cmd = ' '.join([
            'sudo gtdownload -k 15',
            '-c {cghub_key}',
            '-l {aid}.log',
            '-p {download_path}',
            '{aid}', 
        ]).format(
            cghub_key=self.kwargs['cghub_key'], 
            download_path=directory,
            aid=self.work.get('analysis_id'),
        )

        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        output, err = child.communicate()

        if child.returncode:
            logger.error(err)
            raise Exception('Downloader returned with non-zero exit code')

        directory += self.work.get('analysis_id')
        files = [f for f in listdir(directory) if isfile(join(directory,f))]
        self.files = [
            os.path.join(directory, f) for f in files if f.endswith('.bam') or f.endswith('.bai')
        ]
        for f in self.files:
            logger.info('Successfully downloaded file: ' + f)


    def get_bai(self):
        aid = self.work['analysis_id']
        print aid
        result = self.submit([
            'MATCH (n:file)',
            'WHERE n.analysis_id="{aid}"',
            'AND right(n.file_name, 4) = ".bai"',
            'RETURN n',
        ], aid=aid)

        try: 
            return result['data'][0][0]['data']
        except: 
            return None

    def checksum(self):
        self.set_state('CHECK_SUMMING')
        for path in self.files:
            md5 = hashlib.md5()
            with open(path, 'rb') as f:
                for chunk in iter(lambda: f.read(128 * md5.block_size), b''):
                    md5.update(chunk)
                checksum = md5.hexdigest()
            
            work = self.get_bai() if path.endswith('.bai') else self.work
            if not work: continue

            if checksum != work['md5']:
                raise Exception('incorrect checksum')
        self.set_state('CHECK_SUMMED')

    def post(self):
        self.set_state('POSTING')

    def upload(self):
        self.set_state('UPLOADING')

    def get_work(self):
        self.set_state('SCHEDULING')
        while not self.claim_work(): 
            logger.info("Failed to get work, trying again")
        
    def submit(self, cypher, **params):
        if isinstance(cypher, list): cypher = ' '.join(cypher).format(**params)
        data = {"query": cypher}
        r = requests.post(self.url, data=json.dumps(data))
        if r.status_code != 200:
            traceback.print_exc()
            logger.error("FAILURE: cypher query")
            logger.error(json.text)
        return r.json()

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
                'REMOVE n.importer',
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
        self.work = None
