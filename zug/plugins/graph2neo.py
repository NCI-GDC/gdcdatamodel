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

    [{
        'node': {
            'matches': {'key': value}, # the key, values to match when making node
            'node_type': '',
            'body': {}
        }
        'edges': {                    
            'matches': {'key': value }  # the key, values to match when making edge
            'node_type': '',
            'edge_type': '',
    },]

    """
    
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

        src_matches, src_properties, src_cypher = self.parse(src)
        if create_src: self.append_cypher(src_cypher)

        if edge is None: return

        dst_matches, dst_properties, dst_cypher = self.parse(edge)
        self.append_cypher(dst_cypher)

        src_type = src['node_type']
        dst_type = edge['node_type']

        r = 'MATCH (a:{src_type} {{ {src_matches} }}), (b:{dst_type} {{ {dst_matches} }}) '.format(
            src_type=src_type, src_matches=src_matches, dst_type=dst_type, dst_matches=dst_matches)

        r += 'CREATE UNIQUE (a)-[r:{edge_type}]->(b)'.format(edge_type=edge['edge_type'])
        self.append_cypher(r)

    def parse(self, node):

        body = node.get('body', node['matches'])
        cmd = 'MERGE (n:{_type} {{ {matches} }}) ON CREATE SET {properties} ON MATCH SET {properties}'
        matches =  ['{key}:"{value}"'.format(key=key.replace(' ','_'), value=value) for key, value in node['matches'].items()]
        properties = ['n.{key}="{val}"'.format(key=key.replace(' ','_').lower(), val=val) for key, val in body.items()]

        matches = ', '.join(matches)
        properties = ', '.join(properties)
        cypher = cmd.format(_type=node['node_type'], matches=matches, properties=properties)
        return matches, properties, cypher
                
    def append_cypher(self, query):
        self.batch.append({"statement": query})

    def submit(self):
        data = {"statements": self.batch}        
        logger.info("Batch request for {0} statements".format(len(self.batch)))
        r = requests.post(self.url, data=json.dumps(data))
        if r.status_code != 200:
            logger.error("Batch request for {0} statements failed: ".format(len(self.batch)) + r.text)
