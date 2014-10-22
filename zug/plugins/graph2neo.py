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
    
    def initialize(self, **kwargs):
        self.max_retries = kwargs.get('max_retries', 4)
        self.retries = 0

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

        assert 'nodes' in doc, "Graph dictionary must have key 'nodes' with a list of nodes."
        assert 'edges' in doc, "Graph dictionary must have key 'edges' with a list of edges."

        self.batch = []

        for src_id, node in doc['nodes'].iteritems():
            
            self.appendPath(node)

            for dst_id, edge in doc['edges'].get(src_id, {}).iteritems():
                edge_type, dst_type = doc['edges'][src_id][dst_id]
                dest = {'_type': dst_type, 'id': dst_id}
                self.appendPath(node, edge_type, dest, create_src = False)

        nodes = self.submit()
                
    def appendPath(self, src, edge_type = None, dst = None, create_src = True):

        merge = 'MERGE (n:{_type} {{ id:"{id}" }}) ON CREATE SET {properties} ON MATCH SET {properties}'
        properties = lambda node: [
            'n.{key}="{val}"'.format( key=key.replace(' ','_').lower(), val=val) for key, val in node.items()
        ]

        src_type, src_id = src['_type'], src['id']
        if create_src:
            self.append_cypher(merge.format(_type=src_type, id=src_id, properties=', '.join(properties(src))))

        if dst is not None:
            dst_type, dst_id = dst['_type'], dst['id']
            self.append_cypher(merge.format(_type=dst_type, id=dst_id, properties=', '.join(properties(dst))))

        if dst is None or edge_type is None: 
            return 

        r = 'MATCH (a:{src_type} {{ id:"{src_id}" }}), (b:{dst_type} {{ id:"{dst_id}" }}) '.format(
            src_type=src_type, src_id=src_id, dst_type=dst_type, dst_id=dst_id)

        r += 'CREATE UNIQUE (a)-[r:{edge_type}]->(b)'.format(edge_type=edge_type)
        self.append_cypher(r)

    def append_cypher(self, query):
        self.batch.append({"statement": query})

    def submit(self):
        data = {"statements": self.batch}
        url = 'http://localhost:7474/db/data/transaction/commit'
        logger.info("Batch request for {0} statements".format(len(self.batch)))
        print data
        r = requests.post(url, data=json.dumps(data))
        if r.status_code != 200:
            logger.error("Batch request for {0} statements failed: ".format(len(self.batch)) + r.text)
