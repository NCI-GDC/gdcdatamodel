import os
import logging
import copy 

from pprint import pprint

from py2neo import neo4j
import py2neo
import threading 

py2neo.packages.httpstream.http.ConnectionPool._puddles = {}

from zug import basePlugin

currentDir = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(name = "[{name}]".format(name = __name__))

class graph2neo(basePlugin):

    """
    converts a a dictionary of edges and nodes to neo4j
    """
    
    def initialize(self, **kwargs):
        self.db = neo4j.GraphDatabaseService()
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

        # except Exception, msg:
        except py2neo.exceptions.BatchError, msg:
            logger.error("Unable to complete batch neo4j request: %s" % str(msg))
            logger.warn("Trying same document again")
            self.retries += 1
            self.process(doc)

        except Exception, msg:
            logger.error("Unrecoverable error: " + str(msg))
            self.retries = 0
            raise

        else:
            self.retries = 0

    def export(self, doc):

        assert 'nodes' in doc, "Graph dictionary must have key 'nodes' with a list of nodes."
        assert 'edges' in doc, "Graph dictionary must have key 'edges' with a list of edges."

        nodes = []
        rels = {}
        edges = doc['edges']
        ordered_edges = []

        batch = neo4j.WriteBatch(self.db)

        index = 0
        for src_id, node in doc['nodes'].iteritems():

            src_type = node['_type']

            for dst_id, edge in edges[src_id].iteritems():
    
                # Pull edge info gathered before
                edge_type, dst_type = edges[src_id][dst_id]
                dest = {'_type': dst_type, 'id': dst_id}

                # Merge the source node
                properties = ['a.{key} = "{value}"'.format(key=key, value=value) for key, value in node.items()]
                merge = 'MERGE (a:{_type} {{ id:"{id}" }}) ON MATCH SET {properties}'
                a = merge.format(_type=src_type, id=src_id, properties=', '.join(properties))
                batch.append_cypher(a)

                # Merge the destination node
                properties = ['b.{key} = "{value}"'.format(key=key, value=value) for key, value in dest.items()]
                merge = 'MERGE (b:{_type} {{ id:"{id}" }}) ON MATCH SET {properties}'
                b = merge.format(_type=dst_type, id=dst_id, properties=', '.join(properties))
                batch.append_cypher(b)

                # Create a unique relationship between the nodes
                r = 'MATCH (a:{src_type} {{ id:"{src_id}" }}), (b:{dst_type} {{ id:"{dst_id}" }}) '.format(
                    src_type=src_type, src_id=src_id, dst_type=dst_type, dst_id=dst_id)
                r += 'CREATE UNIQUE (a)-[r:{edge_type}]->(b)'.format(edge_type=edge_type)
                batch.append_cypher(r)
                
        nodes = batch.submit()

        return doc

        
