import psqlgraph
from copy import copy, deepcopy
from gdcdatamodel.mappings import (
    participant_tree, participant_traversal,
    file_tree, file_traversal,
    annotation_tree, annotation_traversal,
    ONE_TO_ONE, ONE_TO_MANY
)
from pprint import pprint


class PsqlGraph2JSON(object):

    """
    """

    def __init__(self, psqlgraph_driver):
        """Walks the graph to produce elasticsearch json documents.
        Assumptions include:

        """
        self.graph = psqlgraph_driver
        self.files, self.participants = [], []
        self.batch_size = 10
        self.leaf_nodes = ['center', 'tissue_source_site']
        self.file_tree = self.parse_tree(file_tree, {})
        self.participant_tree = self.parse_tree(participant_tree, {})
        self.annotation_tree = self.parse_tree(annotation_tree, {})

    def parse_tree(self, tree, result):
        for key in tree:
            if key != 'corr':
                result[key] = {}
                self.parse_tree(tree[key], result[key])
        return result

    def _get_base_doc(self, node):
        base = {'{}_id'.format(node.label): node.node_id}
        base.update(node.properties)
        return base

    def walk_tree(self, node, tree, mapping, doc, level=0):
        corr, plural = mapping[node.label]['corr']
        subdoc = self._get_base_doc(node)
        for child in tree[node]:
            child_corr, child_plural = mapping[node.label][child.label]['corr']
            if child_plural not in subdoc and child_corr == ONE_TO_ONE:
                subdoc[child_plural] = {}
            elif child_plural not in subdoc:
                subdoc[child_plural] = []
            self.walk_tree(child, tree[node], mapping[node.label],
                           subdoc[child_plural], level+1)
        if corr == ONE_TO_MANY:
            doc.append(subdoc)
        else:
            doc.update(subdoc)
        return doc

    def copy_tree(self, original, new):
        for node in original:
            new[node] = {}
            self.copy_tree(original[node], new[node])
        return new

    def denormalize_participant(self, node):
        ptree = self.graph.nodes().tree(node.node_id, self.participant_tree)
        participant = self.walk_tree(
            node, ptree, {'participant': participant_tree}, [])[0]
        participant['files'] = []
        for path in participant_traversal['file']:
            for f in self.graph.nodes().ids(node.node_id).path_end(path).all():
                participant['files'].append(self.denormalize_file(
                    f, self.copy_tree(ptree, {})))
        return participant, participant['files']

    def prune_participant(self, relevant_nodes, ptree, keys):
        for node in ptree.keys():
            if node.label not in keys:
                continue
            if ptree[node]:
                self.prune_participant(relevant_nodes, ptree[node])
            if node not in relevant_nodes:
                ptree.pop(node)

    def denormalize_file(self, f, ptree):
        doc = self._get_base_doc(f)
        relevant = {}
        base = self.graph.nodes().ids(f.node_id)
        union = base
        for path in file_traversal.participant:
            union = union.union(base.path_whole(path))
        for n in union.all():
            relevant[n.node_id] = n
        self.prune_participant(relevant, ptree, [
            'sample', 'portion', 'analyte', 'aliquot', 'file'])
        participants = []
        for node in ptree.keys():
            participants.append(self.walk_tree(
                node, ptree, {'participant': participant_tree}, []))
        doc['participants'] = participants
        return doc

    def denormalize_annotation(self, a):
        raise NotImplementedError()

    def get_nodes(self, label):
        return self.graph.nodes()\
                         .labels(label)\
                         .yield_per(self.batch_size)

    def walk_participants(self):
        with self.graph.session_scope():
            for p in self.get_participants():
                yield self.denormalize_participant(p)

    def walk_files(self):
        with self.graph.session_scope():
            for f in self.get_files():
                yield self.denormalize_file(f)
