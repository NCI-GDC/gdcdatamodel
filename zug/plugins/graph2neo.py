import os
import logging

from pprint import pprint

from py2neo import neo4j
import py2neo

from zug import basePlugin

currentDir = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(name = "[{name}]".format(name = __name__))

class graph2neo(basePlugin):

    """
    
    """
    
    def initialize(self, **kwargs):
        self.db = neo4j.GraphDatabaseService()
        self.max_retries = kwargs.get('max_retries', 4)
        self.retries = 0

    def next(self, doc):

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
            self.next(doc)

        except Exception, msg:
            logger.error("Unrecoverable error: " + str(msg))
            self.retries = 0
            raise

        else:
            self.retries = 0

        return doc

    def export(self, doc):

        assert 'nodes' in doc, "Graph dictionary must have key 'nodes' with a list of nodes."
        assert 'edges' in doc, "Graph dictionary must have key 'edges' with a list of edges."

        nodes = []
        rels = {}
        edges = doc['edges']
        ordered_edges = []

        batch = neo4j.WriteBatch(self.db)

        for src_id, node in doc['nodes'].iteritems():

            src_type = node['_type']

            for dst_id, edge in edges[src_id].iteritems():
    
                # Pull edge info gathered before
                edge_type, dst_type = edges[src_id][dst_id]
                dest = {'_type': dst_type, 'id': dst_id}
    
                # create nodes
                src_node = py2neo.node(node)
                dst_node = py2neo.node(dest)
    
                # Get or create index, source node, and destination node
                index = self.db.get_or_create_index(neo4j.Node, src_type)
                src = batch.get_or_create_indexed_node(src_type, 'id', src_id, src_node)
                dst = batch.get_or_create_indexed_node(dst_type, 'id', dst_id, dst_node)
                ordered_edges.append(edge_type)
    
        nodes = batch.submit()

        batch = neo4j.WriteBatch(self.db)
        add_rel = "START n=node({src}), m=node({dst}) CREATE UNIQUE (n)-[r:{edge_type}]->(m)"
        for i in range(0, len(nodes)-1, 2):
            src = nodes[i]._id
            dst = nodes[i+1]._id
            batch.append_cypher(add_rel.format(src=src, dst=dst, edge_type=ordered_edges[i/2]))
        nodes = batch.submit()

        return doc

        
