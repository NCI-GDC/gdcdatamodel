import os, imp, requests, logging, re
from pprint import pprint
from py2neo import neo4j
import py2neo

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('PipelinePlugin', basePath)
logger     = logging.getLogger(name = "[{name}]".format(name = __name__))

bcr = imp.load_source('bcr', os.path.join(currentDir, 'bcr.py'))

class PipelinePlugin(base.PipelinePluginBase):

    """
    
    """
    
    def initialize(self, host = 'localhost', port = 7474, user = None, password = None, **kwargs):
        self.db = neo4j.GraphDatabaseService()

    def next(self, doc):

        assert 'nodes' in doc, "Graph dictionary must have key 'nodes' with a list of nodes."
        assert 'edges' in doc, "Graph dictionary must have key 'edges' with a list of edges."

        batch = neo4j.WriteBatch(self.db)
        
        nodes = []
        rels = {}
        edge_types = []

        for edge_type, edges in doc['edges'].iteritems():
            # walk the edges once to create key value mapping by id
            for edge in edges:
                origin, dest = edge
                node_type, id = origin
                if node_type not in rels: rels[node_type] = {}
                rels[node_type][id] = edge_type, dest

        for id, node in doc['nodes'].iteritems():
            node_type = node.get('_type', None)

            edge_type, dest = rels[node_type][id]
            dest_type, dest_id = dest
            dest = {'_type': dest_type, 'id': dest_id}

            # create nodes
            src_node = py2neo.node(node)
            dst_node = py2neo.node(dest)

            # Get or create index, source node, and destination node
            index = self.db.get_or_create_index(neo4j.Node, node_type)
            src = batch.get_or_create_indexed_node(node_type, 'id', id, src_node)
            dst = batch.get_or_create_indexed_node(dest_type, 'id', dest_id, dst_node)

            # Keep a record of the edge type in order
            edge_types.append(edge_type)

        nodes = batch.submit()

        batch = neo4j.WriteBatch(self.db)
        add_rel = "START n=node({src}), m=node({dst}) create unique (n)-[r:{edge_type}]->(m)"
        for i in range(0, len(nodes)-1, 2):
            src = nodes[i]._id
            dst = nodes[i+1]._id
            batch.append_cypher(add_rel.format(src=src, dst=dst, edge_type=edge_types[i/2]))

        rels = batch.submit()

        return doc

        
