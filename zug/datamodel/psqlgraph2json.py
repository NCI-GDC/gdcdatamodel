import psqlgraph
from copy import copy
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
        corr, plural = mapping[node.label].corr
        subdoc = self._get_base_doc(node)
        for child in tree[node]:
            child_corr, child_plural = mapping[node.label][child.label].corr
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

    def _walk_path(self, node, path):
        return [self._get_base_doc(n)
                for n in self.graph.nodes().ids_path_end(node.node_id, path)]

    def walk_paths(self, node, traversals, mapping, doc):
        subdoc = self._get_base_doc(node)
        for dst, paths in traversals.items():
            corr, name = mapping[dst].corr
            props = []
            for path in paths:
                props += self._walk_path(node, path)
            props = [dict(t) for t in set([tuple(d.items()) for d in props])]
            if corr == ONE_TO_ONE and props:
                assert len(props) <= 1, props
                subdoc[name] = props[0]
            elif props:
                if name not in subdoc:
                    subdoc[name] = []
                subdoc[name] += props
        return self.update_doc(doc, subdoc)

    def denormalize_participant(self, node):
        ptree = self.graph.nodes().tree(node.node_id, self.participant_tree)
        return self.walk_tree(
            node, ptree, {'participant': participant_tree}, [])

        # files = []
        # p = self.walk_paths(node, participant_traversal, participant_tree, {})
        # for f in p.get('files', []):
        #     f['participant'] = copy(participant)
        #     files.append(f)
        # participant['files'] = files

    def denormalize_file(self, f):
        res = self.walk_tree(f, file_tree, {}, [])
        res['participants'] = []
        paths = self.walk_paths(f, file_traversal, file_tree, {})
        participants = paths.pop('participants', [])
        for p_dict in participants:
            participant = self.walk_tree(
                self.graph.nodes().ids(p_dict['participant_id']).one(),
                participant_tree, {}, [])
            res['participants'].append(participant)
        return res

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
