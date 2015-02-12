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
        self.g = psqlgraph_driver
        self.files, self.participants = [], []
        self.batch_size = 10
        self.leaf_nodes = ['center', 'tissue_source_site']
        self.file_tree = self.parse_tree(file_tree, {})
        self.file_tree.pop('data_subtype')
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

    def combine_paths_from(self, ids, paths, labels=None, base=None):
        if not base:
            base = self.g.nodes().ids(ids)
        union = base
        for path in paths:
            union = union.union(base.path_end(path))
        if labels:
            return union.labels(labels)
        else:
            return labels

    def denormalize_participant(self, node):
        ptree = self.g.nodes().tree(node.node_id, self.participant_tree)
        participant = self.walk_tree(
            node, ptree, {'participant': participant_tree}, [])[0]
        participant['files'] = []
        files = self.combine_paths_from(
            node.node_id, participant_traversal['file'], 'file').all()
        get_file = lambda f: self.denormalize_file(
            f, self.copy_tree(ptree, {}))
        participant['files'] = map(get_file, files)
        return participant, participant['files']

    def prune_participant(self, relevant_nodes, ptree, keys):
        for node in ptree.keys():
            if node.label not in keys:
                continue
            if ptree[node]:
                self.prune_participant(relevant_nodes, ptree[node])
            if node not in relevant_nodes:
                ptree.pop(node)

    def get_data_type_tree(self, f):
        data_subtype = self.g.nodes().ids(
            f.node_id).path_end(['data_subtype']).first()
        data_type = self.g.nodes().ids(
            f.node_id).path_end(['data_subtype', 'data_type']).first()
        if data_subtype and not data_type:
            return {data_subtype: {}}
        elif data_type:
            return {data_subtype: {data_type: {}}}
        return {}

    def denormalize_file(self, f, ptree):
        ftree = self.g.nodes().tree(f.node_id, self.file_tree)
        ftree[f].update(self.get_data_type_tree(f))
        doc = self.walk_tree(f, ftree, {'file': file_tree}, [])[0]
        relevant = {}
        base = self.g.nodes().ids(f.node_id)
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
        return self.g.nodes()\
                     .labels(label)\
                     .yield_per(self.batch_size)

    def walk_participants(self):
        with self.g.session_scope():
            for p in self.get_participants():
                yield self.denormalize_participant(p)

    def walk_files(self):
        with self.g.session_scope():
            for f in self.get_files():
                yield self.denormalize_file(f)

    def denormalize_project(self, p):
        doc = self._get_base_doc(p)

        # Get programs
        program = self.g.nodes().ids(p.node_id).path_end('program').first()

        # Get participants
        if program:
            doc['program'] = self._get_base_doc(program)
        parts = self.g.nodes().ids(p.node_id).path_end('participant').all()

        # Get experimental strategies
        exp_strat_paths = [list(path) + ['experimental_strategy']
                           for path in participant_traversal['file']]
        exp_strats = self.combine_paths_from(
            [part.node_id for part in parts],
            exp_strat_paths, 'experimental_strategy').all()
        exp_strat_summaries = []
        for exp_strat in exp_strats:
            exp_strat_summaries.append({
                'participant_count': self.combine_paths_from(
                    exp_strat.node_id,
                    [path[::-1]+['participant'] for path in exp_strat_paths],
                    'participants').count(),
                'file_count': self.g.nodes().ids(
                    exp_strat.node_id).path_end('file').count(),
                'experimental_strategy': exp_strat['name'],
            })

        # Get files
        files = self.combine_paths_from(
            [part.node_id for part in parts],
            participant_traversal['file'], 'file').all()

        # Get data types
        data_type_summaries = []
        data_type_paths = [['data_subtype']+list(path)[::-1]+['participant']
                           for path in participant_traversal['file']]
        data_types = self.g.nodes().ids(
            [f.node_id for f in files]).path_end(
            ['data_subtype', 'data_type']).all()
        for data_type in data_types:
            data_type_summaries.append({
                'participant_count': self.combine_paths_from(
                    [d.node_id for d in data_types],
                    data_type_paths, 'participant').count(),
                'data_type': data_type['name'],
                'file_count': self.combine_paths_from(
                    [d.node_id for d in data_types],
                    ['data_subtype', 'file'], 'participant').count(),
            })

        # Compile summary
        doc['summary'] = {
            'participant_count': len(parts),
            # 'experimental_strategies': exp_strat_summaries,
            'file_count': len(files),
            'file_size': sum([f['file_size'] for f in files]),
            'data_types': data_type_summaries,
        }
        return doc
