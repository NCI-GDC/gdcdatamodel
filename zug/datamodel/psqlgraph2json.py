import psqlgraph
import itertools
import json
from pprint import pprint
from gdcdatamodel.mappings import participant_tree, get_file_es_mapping, \
    get_participant_es_mapping, ONE_TO_ONE, ONE_TO_MANY


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
        if mapping[label].corr == ONE_TO_ONE:
            doc[label] = {}
        elif mapping[label].corr == ONE_TO_MANY:
            doc[label] = []
        else:
            raise RuntimeError('Unknown correspondence for {} {}'.format(
                label, mapping[label].corr))

    def walk_participant_node(self, node, mapping, doc=[], path=[], level=0):
        # print '|  '*level, '+', node.label, mapping.corr
        subdoc = {k: v for k, v in node.properties.iteritems()}
        for neighbor, label in itertools.chain(
                [(a.src, a.label) for a in node.edges_in],
                [(b.dst, b.label) for b in node.edges_out]):
            if neighbor.label not in mapping.keys()\
               or node.node_id in path or neighbor.label in self.leaf_nodes:
                continue
            self.add_child(mapping, neighbor.label, subdoc)
            self.walk_participant_node(
                neighbor, mapping[neighbor.label], subdoc[neighbor.label],
                path+[node.node_id], level+1)
        if isinstance(doc, list):
            doc.append(subdoc)
        elif isinstance(doc, dict):
            doc.update(subdoc)
        else:
            raise RuntimeError('Unexpected document type')
        return doc

    def get_participants(self):
        self.participants = self.graph.node_lookup(label='participant')\
                                      .yield_per(self.batch_size)  # .limit(2)

    def walk_participants(self):
        self.get_participants()
        docs = []
        for f in self.participants:
            docs.append(self.walk_participant_node(f, participant_tree))
        print json.dumps(docs, indent=2)
