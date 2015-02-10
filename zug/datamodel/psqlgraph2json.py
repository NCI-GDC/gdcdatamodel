import psqlgraph
from copy import copy
from gdcdatamodel.mappings import (
    participant_tree, participant_traversal,
    file_tree, file_traversal,
    annotation_tree, annotation_traversal,
    ONE_TO_ONE, ONE_TO_MANY
)


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

    def add_child(self, mapping, label, doc):
        if label not in mapping:
            return None
        plural = mapping[label].corr[1]
        if plural in doc:
            return plural
        elif mapping[label].corr[0] == ONE_TO_ONE:
            doc[plural] = {}
        elif mapping[label].corr[0] == ONE_TO_MANY:
            doc[plural] = []
        else:
            raise RuntimeError('Unknown correspondence for {} {}'.format(
                label, mapping[label].corr))
        return plural

    def update_doc(self, doc, subdoc):
        if doc is None:
            return subdoc
        if isinstance(doc, list):
            doc.append(subdoc)
        elif isinstance(doc, dict):
            doc.update(subdoc)
        else:
            raise RuntimeError('Unexpected document type')
        return doc

    def _get_base_doc(self, node):
        base = {'{}_id'.format(node.label): node.node_id}
        base.update(node.properties)
        return base

    def _is_walkable(self, node, path):
        return (node.node_id not in path
                and node.label not in self.leaf_nodes)

    def _get_neighbors(self, node, mapping):
        return self.graph.nodes()\
                         .ids(node.node_id)\
                         .neighbors()\
                         .labels(mapping.keys()).all()

    def walk_tree(self, node, mapping, doc, path=[], level=0):
        subdoc = self._get_base_doc(node)
        for n in self._get_neighbors(node, mapping):
            plural = self.add_child(mapping, n.label, subdoc)
            if self._is_walkable(n, path) and plural:
                self.update_doc(subdoc[plural], self.walk_tree(
                    n, mapping[n.label], {}, path+[node.node_id], level+1))
            else:
                subdoc[plural]['{}_id'.format(n.label)] = n.node_id
                self.update_doc(subdoc[plural], n.properties)
        return self.update_doc(doc, subdoc)

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
        participant = self.walk_tree(node, participant_tree, {})
        files = []
        p = self.walk_paths(node, participant_traversal, participant_tree, {})
        for f in p.get('files', []):
            f['participant'] = copy(participant)
            files.append(f)
        participant['files'] = files
        return participant

    def denormalize_file(self, f):
        res = self.walk_tree(f, file_tree, {})
        res['participants'] = []
        paths = self.walk_paths(f, file_traversal, file_tree, {})
        participants = paths.pop('participants', [])
        for p_dict in participants:
            participant = self.walk_tree(
                self.graph.nodes().ids(p_dict['participant_id']).one(),
                participant_tree, {})
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
