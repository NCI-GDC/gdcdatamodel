import psqlgraph
from psqlgraph import PsqlNode
import itertools
import json
from pprint import pprint
from gdcdatamodel.mappings import participant_tree, file_tree, file_traversal,\
    get_file_es_mapping, get_participant_es_mapping, ONE_TO_ONE, ONE_TO_MANY


class PsqlGraph2JSON(object):

    """
    """

    def __init__(self, host, user, password, database,
                 node_validator=None):
        """Walks the graph to produce elasticsearch json documents.
        Assumptions include:

        """
        self.graph = psqlgraph.PsqlGraphDriver(
            host=host, user=user, password=password, database=database)
        self.files, self.participants = [], []
        self.batch_size = 10
        self.leaf_nodes = ['center', 'tissue_source_site']
        # self.file_mapping = get_file_es_mapping()
        # self.participant_mapping = get_participant_es_mapping()

    def add_child(self, mapping, label, doc):
        if mapping[label].corr[0] == ONE_TO_ONE:
            doc[mapping[label].corr[1]] = {}
        elif mapping[label].corr[0] == ONE_TO_MANY:
            doc[mapping[label].corr[1]] = []
        else:
            raise RuntimeError('Unknown correspondence for {} {}'.format(
                label, mapping[label].corr))
        return mapping[label].corr[1]

    def update_doc(self, doc, subdoc):
        if doc is None:
            return subdoc
        if isinstance(doc, list):
            doc.append(subdoc)
        elif isinstance(doc, dict):
            doc.update(subdoc)
        else:
            raise RuntimeError('Unexpected document type')
        return subdoc

    def walk_tree(self, node, mapping, doc=None, path=[], level=0):
        # print '|  '*level, '+', node.label, mapping.corr
        subdoc = {k: v for k, v in node.properties.iteritems()}
        for neighbor, label in itertools.chain(
                [(a.src, a.label) for a in node.edges_in],
                [(b.dst, b.label) for b in node.edges_out]):
            if neighbor.label not in mapping.keys()\
               or node.node_id in path or neighbor.label in self.leaf_nodes:
                continue
            label = self.add_child(mapping, neighbor.label, subdoc)
            self.walk_tree(neighbor, mapping[label], subdoc[label],
                           path+[node.node_id], level+1)
        doc = self.update_doc(doc, subdoc)
        return doc

    def walk_paths(self, node, traversals, mapping, doc=None):
        subdoc = {k: v for k, v in node.properties.iteritems()}
        for src, paths in traversals.items():
            corr, name = mapping[src].corr
            for path in paths.get('path_in', []):
                print src, path
                props = [n.properties for n in
                         self.graph.nodes().labels(src).path_in(path)
                         .ids(node.node_id).all()]
                if corr == ONE_TO_ONE and props:
                    assert len(props) <= 1
                    subdoc[name] = props[0]
                elif props:
                    if name not in subdoc:
                        subdoc[name] = props
                    else:
                        subdoc[name] += props
        doc = self.update_doc(doc, subdoc)
        return doc

    def get_participants(self):
        self.participants = self.graph.nodes().labels('participant')\
                                              .yield_per(self.batch_size)\
                                              .limit(2)

    def get_files(self):
        self.files = self.graph.nodes().labels('file')\
                                       .yield_per(self.batch_size)\
                                       .limit(1)

    def walk_participants(self):
        with self.graph.session_scope():
            self.get_participants()
            docs = []
            for f in self.participants:
                participant = self.walk_tree(f, participant_tree)
                docs.append(participant)
                print f.node_id
                print json.dumps(docs, indent=2)

    def walk_files(self):
        with self.graph.session_scope():
            self.get_files()
            docs = []
            for f in self.files:
                docs.append(self.walk_paths(f, file_traversal, file_tree))
                print json.dumps(docs, indent=2)
