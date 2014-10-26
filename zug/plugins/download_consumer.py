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

from lxml import etree
from pprint import pprint
from zug import basePlugin
from zug.exceptions import IgnoreDocumentException, EndOfQueue

currentDir = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(name = "[{name}]".format(name = __name__))


class download_consumer(basePlugin):

    """
    takes in an xml as a string and compiles a list of nodes and edges
    """
    
    def initialize(self, **kwargs):
        self.id = str(uuid.uuid4())
        self.state = 'IDLE'
        self.check_count = int(kwargs.get('check_count', 5))
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', '7474')
        self.url = 'http://{host}:{port}/db/data/cypher'.format(host=self.host, port=self.port)

    def process(self, doc):
        pass


    def verify_claim(self, file_id):

        time.sleep(random.random()*10 + 1)
        result = self.submit(' '.join([
            'MATCH (n:file {{id:"{file_id}"}})',
            'WHERE n.importer="{id}"',
            'RETURN n', 
        ]).format(file_id=file_id, id=self.id))

        if len(result['data']) == 0:
            return False

        return True

    def get_work(self):

        self.state = 'SCHEDULING'
        result = self.submit(' '.join([
            'MATCH (n:file)',
            'WHERE n.import_state="NOT_STARTED"'
            'OR n.import_state="ERROR"'
            'AND right(n.file_name, 4) <> ".bai"',
            'WITH n LIMIT 1',
            'RETURN n',
            ]))

        if not len(result['data']):
            raise Exception("No more work")

        file_id = result['data'][0][0]['data']['id']

        result = self.submit(' '.join([
            'MATCH (n:file {{id:"{file_id}"}})',
            'WHERE n.import_state="NOT_STARTED"'
            'OR n.import_state="ERROR"',
            'SET n.importer="{id}", n.import_state="STARTED"',
            'RETURN n', 
        ]).format(file_id=file_id, id=self.id))
        
        for i in range(self.check_count):
            if not self.verify_claim(file_id): 
                return False                
                
        self.work = result['data'][0][0]['data']
        self.state = 'SCHEDULED'
        return True

    def finish_work(self):

        result = self.submit(' '.join([
            'MATCH (n:file {{id:"{file_id}"}})',
            'SET n.import_state="COMPLETE"',
            'REMOVE n.importer',
        ]).format(file_id=self.work['id']))
        
    def start(self):

        while not self.get_work(): 
            logger.info("Failed to get work, trying again")

        print self.work['legacy_url']
        self.finish_work()

    def submit(self, cypher):
        data = {"query": cypher}
        r = requests.post(self.url, data=json.dumps(data))
        if r.status_code != 200:
                    logger.info("FAILURE: cypher query")
        return r.json()
