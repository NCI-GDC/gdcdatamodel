import psqlgraph
import itertools
from gdcdatamodel.mappings import get_file_es_mapping, \
    get_participant_es_mapping


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
        self.leaf_nodes = ['center']
        self.file_mapping = get_file_es_mapping()
        self.participant_mapping = get_participant_es_mapping()

    def walk_participant_node(self, node, mapping, path=[], level=0):
        for neighbor, label in itertools.chain(
                [(a.src, a.label) for a in node.edges_in],
                [(b.dst, b.label) for b in node.edges_out]):
            if neighbor.label not in mapping.keys()\
               or node.node_id in path or neighbor.label in self.leaf_nodes:
                continue
            self.walk_participant_node(
                neighbor, mapping[neighbor.label],
                path+[node.node_id], level+1)

    def get_participants(self):
        self.participants = self.graph.node_lookup(label='participant')\
                                      .yield_per(self.batch_size)\
                                      .limit(1)

    def walk_participants(self):
        self.get_participants()
        for f in self.participants:
            self.walk_participant_node(f, self.participant_mapping)
