import psqlgraph
from psqlgraph import PsqlNode
import itertools
import json
from copy import copy
from pprint import pprint
from gdcdatamodel.mappings import \
    participant_tree, participant_traversal,\
    file_tree, file_traversal,\
    get_file_es_mapping, get_participant_es_mapping, \
    ONE_TO_ONE, ONE_TO_MANY


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
        # print('|  '*level, '+', node.label, mapping.corr)
        subdoc = {'uuid': node.node_id}
        subdoc.update(node.properties)
        for neighbor, label in itertools.chain(
                [(a.src, a.label) for a in node.edges_in],
                [(b.dst, b.label) for b in node.edges_out]):
            if neighbor.label not in mapping.keys()\
               or node.node_id in path or neighbor.label in self.leaf_nodes:
                continue
            new_label = self.add_child(mapping, neighbor.label, subdoc)
            self.walk_tree(neighbor, mapping[neighbor.label],
                           subdoc[new_label], path+[node.node_id], level+1)
        doc = self.update_doc(doc, subdoc)
        return doc

    def _walk_path(self, node, path):
        props = []
        for node in self.graph.nodes().ids(node.node_id).path_end(path).all():
            nprop = {'uuid': node.node_id}
            nprop.update(node.properties)
            props.append(nprop)
        return props

    def walk_paths(self, node, traversals, mapping, doc=None):
        subdoc = {'uuid': node.node_id}
        subdoc.update(node.properties)
        for dst, paths in traversals.items():
            corr, name = mapping[dst].corr
            props = []
            for path in paths:
                props += self._walk_path(node, path)
            # remove duplicates
            props = [dict(t) for t in set([tuple(d.items()) for d in props])]
            if corr == ONE_TO_ONE and props:
                assert len(props) <= 1, props
                subdoc[name] = props[0]
            elif props:
                if name not in subdoc:
                    subdoc[name] = []
                subdoc[name] += props
        doc = self.update_doc(doc, subdoc)
        return doc

    def get_participants(self):
        self.participants = self.graph.nodes()\
                                      .labels('participant')\
                                      .yield_per(self.batch_size)\
                                      .limit(1)

    def get_files(self):
        self.files = self.graph.nodes()\
                               .labels('file')\
                               .yield_per(self.batch_size)\
                               .limit(1)

    def walk_participant(self, node):
        participant = self.walk_tree(node, participant_tree)
        files = []
        for f in self.walk_paths(node, participant_traversal,
                                 participant_tree)['files']:
            f['participant'] = copy(participant)
            files.append(f)
        participant['files'] = files
        return participant

    def walk_file(self, f):
        res = self.walk_tree(f, file_tree)
        paths = self.walk_paths(f, file_traversal, file_tree)
        res.update(paths)
        p_dict = paths.pop('participant', None)
        if p_dict:
            participant = self.walk_tree(
                self.graph.nodes().ids(p_dict['uuid']).one(),
                participant_tree)
            res['participant'] = participant
        return res

    def walk_participants(self):
        with self.graph.session_scope():
            self.get_participants()
            docs = []
            for p in self.participants:
                docs.append(self.walk_participant(p))
            return docs

    def walk_files(self):
        with self.graph.session_scope():
            self.get_files()
            docs = []
            for f in self.files:
                docs.append(self.walk_file(f))
            return docs
