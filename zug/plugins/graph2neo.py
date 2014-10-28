import os
import logging
import copy 
import sys
import requests
import json

from pprint import pprint

from zug import basePlugin

currentDir = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(name = "[{name}]".format(name = __name__))

class graph2neo(basePlugin):

    """
    converts a a dictionary of edges and nodes to neo4j

    """

    [{
        'node': {
            'matches': {'key': 'value'}, # the key, values to match when making node
            'node_type': '',
            'body': {}, # these values will be written/overwritten
            'on_match': {}, # these values will only be written if the node ALREADY exists
            'on_create': {}, # these values will only be written if the node DOES NOT exist
        },
        'edges': {                    
            'matches': {'key': 'value' },  # the key, values to match when making edge
            'node_type': '',
            'edge_type': '',
        }
    },]
    
    def initialize(self, **kwargs):
        self.max_retries = kwargs.get('max_retries', 4)
        self.retries = 0

        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', '7474')

        self.url = 'http://{host}:{port}/db/data/transaction/commit'.format(host=self.host, port=self.port)

    def process(self, doc):

        if self.retries > self.max_retries: 
            logger.error("Exceeded number of max retries! Aborting: [{r} > {m}]".format(
                    r=self.retries, m=self.max_retries))
            self.retries = 0
            return doc

        try:
            self.export(doc)

        except Exception, msg:
            logger.error("Unrecoverable error: " + str(msg))
            self.retries = 0
            raise

        else:
            self.retries = 0

    def export(self, doc):

        self.batch = []
        for node in doc:            
            src = node['node']
            self.appendPath(src)
            for edge in node['edges']:
                self.appendPath(src, edge, create_src = False)

        nodes = self.submit()

    def appendPath(self, src, edge = None, create_src = True):

        src_statement = self.parse(src)
        if create_src: self.append_statement(src_statement)

        if edge is None: return

        dst_statement = self.parse(edge)
        self.append_statement(dst_statement)

        src_type = src['node_type']
        dst_type = edge['node_type']

        match_lst =  ['{key}:{{src_matches}}.{key}'.format(key=key.replace(' ','_')) for key, val in src['matches'].items()]
        src_matches = ', '.join(match_lst)
        match_lst =  ['{key}:{{dst_matches}}.{key}'.format(key=key.replace(' ','_')) for key, val in edge['matches'].items()]
        dst_matches = ', '.join(match_lst)

        cmd = 'MATCH (a:{src_type} {{ {src_matches} }}), (b:{dst_type} {{ {dst_matches} }}) '.format(
            src_type=src_type,
            src_matches=src_matches,
            dst_type=dst_type,
            dst_matches=dst_matches
        )
        cmd += 'CREATE UNIQUE (a)-[r:{edge_type}]->(b)'.format(edge_type=edge['edge_type'])

        statement = {
            "statement": cmd,
            "parameters": {
                "src_matches": src['matches'],
                "dst_matches": edge['matches'],
            }
        }
        self.append_statement(statement)

    def parse(self, node):

        body = node.get('body', copy.copy(node['matches']))
        on_match = node.get('on_match', {})
        on_create = node.get('on_create', copy.copy(node['matches']))
        
        if '_type' not in on_create and '_type' not in body:
            on_create['_type'] = node['node_type']

        for key, value in body.items():
            on_match[key] = value
            on_create[key] = value

        match_lst =  ['{key}:{{matches}}.{key}'.format(key=key.replace(' ','_')) for key, val in node['matches'].items()]
        matches = ', '.join(match_lst)

        cmd = 'MERGE (n:{_type} {{{matches}}}) ON CREATE SET n += {{on_create}} '.format(
            matches=matches,
            _type=node['node_type'])

        if len(on_match) != 0: 
            cmd += 'ON MATCH SET n += {on_match}'

        statement = {
            "statement": cmd,
            "parameters": {
                "matches": node['matches'],
                "on_create": { key.replace(' ','_').lower(): val for key, val in on_create.items() }
            }
        }

        if len(on_match) != 0: 
            statement['parameters']['on_match'] = { key.replace(' ','_').lower(): val for key, val in on_create.items() }
        
        return statement

    def append_statement(self, statement):
        self.batch.append(statement)

    def submit(self):
        data = {"statements": self.batch}        
        logger.info("Batch request for {0} statements".format(len(self.batch)))
        r = requests.post(self.url, data=json.dumps(data))
        if r.status_code != 200:
            logger.error("Batch request for {0} statements failed: ".format(len(self.batch)) + r.text)
        logger.info("Batch request for {0} statements returned successfully".format(len(self.batch)))
