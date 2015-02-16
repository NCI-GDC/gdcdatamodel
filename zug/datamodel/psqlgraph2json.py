from gdcdatamodel.mappings import (
    participant_tree, participant_traversal,
    file_tree, file_traversal,
    annotation_tree, annotation_traversal,
    ONE_TO_ONE, ONE_TO_MANY
)
import logging
from cdisutils.log import get_logger
import networkx as nx
from psqlgraph import Node, Edge
from pprint import pprint
import itertools
from sqlalchemy.orm import joinedload

log = get_logger("psqlgraph2json")
log.setLevel(level=logging.INFO)


class PsqlGraph2JSON(object):

    """
    """

    def __init__(self, psqlgraph_driver):
        """Walks the graph to produce elasticsearch json documents.
        Assumptions include:

        """
        self.g = psqlgraph_driver
        self.G = nx.Graph()
        self.files, self.participants = [], []
        self.batch_size = 10
        self.leaf_nodes = ['center', 'tissue_source_site']
        self.file_tree = self.parse_tree(file_tree, {})
        self.file_tree.pop('data_subtype')
        self.participant_tree = self.parse_tree(participant_tree, {})
        self.annotation_tree = self.parse_tree(annotation_tree, {})
        self.ptree_mapping = {'participant': participant_tree.to_dict()}
        self.ftree_mapping = {'file': file_tree.to_dict()}

    def cache_database(self):
        log.info('Caching database ...')
        log.info('Caching {} nodes ...'.format(self.g.nodes().count()))
        log.info('Caching {} edges ...'.format(self.g.edges().count()))
        for e in self.g.edges().options(joinedload(Edge.src))\
                               .options(joinedload(Edge.dst))\
                               .yield_per(10000):
            self.G.add_edge(e.src, e.dst)
        log.info('Cached {} edges'.format(self.G.number_of_edges()))

    def load_cache_from_disk(self, path):
        log.info('Loading cache from disk ...')
        self.G = nx.read_gpickle(path)
        log.info('Cached {} nodes'.format(self.G.number_of_nodes()))

    def nodes_labeled(self, label):
        for n, p in self.G.nodes_iter(data=True):
            if n.label == label:
                yield n

    def neighbors_labeled(self, node, label):
        for n in self.G.neighbors(node):
            if n.label == label:
                yield n

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

    def create_tree(self, node, mapping, tree):
        submap = mapping.get(node.label)
        corr, plural = submap['corr']
        for child in self.G[node]:
            if child.label not in submap:
                continue
            tree[child] = {}
            self.create_tree(child, submap, tree[child])
        return tree

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

    def walk_path(self, node, path, whole=False, level=0):
        if path:
            for neighbor in self.neighbors_labeled(node, path[0]):
                for n in self.walk_path(neighbor, path[1:], whole, level+1):
                    yield n
        if whole or (len(path) == 1 and path[0] == node.label):
            yield node

    def walk_paths(self, node, paths, whole=False):
        return {n for n in itertools.chain(
            *[self.walk_path(node, path, whole)
              for path in paths])}

    def get_exp_strats(self, files):
        exp_strats = {}
        for n in files:
            for exp_strat in self.walk_path(n, ['experimental_strategy']):
                name = exp_strat['name']
                if name not in exp_strats:
                    exp_strats[name] = {
                        'expertimental_strategy': name,
                        'file_count': 0}
                exp_strats[name]['file_count'] += 1
        return exp_strats.values()

    def get_data_types(self, files):
        exp_strats = {}
        for n in files:
            for exp_strat in self.walk_path(n, ['data_subtype', 'data_type']):
                name = exp_strat['name']
                if name not in exp_strats:
                    exp_strats[name] = {
                        'data_type': name,
                        'file_count': 0}
                exp_strats[name]['file_count'] += 1
        return exp_strats.values()

    def denormalize_participant(self, node):
        ptree = {node: self.create_tree(node, self.ptree_mapping, {})}
        participant = self.walk_tree(node, ptree, self.ptree_mapping, [])[0]
        files = self.walk_paths(node, participant_traversal['file'])
        participant['summary'] = {
            'data_file_count': len(files),
            'file_size': sum([f['file_size'] for f in files]),
            'expertimental_strategies': self.get_exp_strats(files),
            'data_types': self.get_data_types(files),
        }
        get_file = lambda f: self.denormalize_file(
            f, self.copy_tree(ptree, {}))
        participant['files'] = map(get_file, files)
        return participant, participant['files']

    def prune_participant(self, relevant_nodes, ptree, keys):
        for node in ptree.keys():
            if ptree[node]:
                self.prune_participant(relevant_nodes, ptree[node], keys)
            if node.label in keys and node not in relevant_nodes:
                ptree.pop(node)

    def denormalize_file(self, node, ptree):
        ftree = {node: self.create_tree(node, self.ftree_mapping, {})}
        doc = self.walk_tree(node, ftree, self.ftree_mapping, [])[0]
        relevant = self.walk_paths(node, file_traversal.participant, True)
        self.prune_participant(relevant, ptree, [
            'sample', 'portion', 'analyte', 'aliquot', 'file'])
        doc['participants'] = map(
            lambda p: self.walk_tree(p, ptree, self.ptree_mapping, [])[0],
            ptree)
        return doc

    def denormalize_annotation(self, a):
        raise NotImplementedError()

    def denormalize_project(self, p):
        """
        This is a pretty crazy graph traversal.

        """
        doc = self._get_base_doc(p)

        # Get programs
        program = self.g.nodes().ids(p.node_id).path_end('program').first()

        # Get participants
        log.info('Query for programs')
        if program:
            doc['program'] = self._get_base_doc(program)
        parts = self.g.nodes().ids(p.node_id).path_end('participant').all()

        # Construct paths
        paths = [
            ['file'],
            ['sample', 'file'],
            ['sample', 'aliquot', 'file'],
            ['sample', 'portion', 'file'],
            ['sample', 'portion', 'analyte', 'file'],
            ['sample', 'portion', 'analyte', 'aliquot', 'file'],
        ]
        data_type_to_part_paths = [
            ['data_subtype'] + path[::-1]+['participant'] for path in paths]
        exp_strat_to_part_paths = [
            path[::-1]+['participant'] for path in paths]
        file_to_data_type_path = ['data_subtype', 'data_type']

        # Get files
        log.info('Query for files')
        files = self.combine_paths_from(
            [part.node_id for part in parts],
            paths, 'file').all()

        # Get experimental strategies
        log.info('Query for experimental strategies')
        exp_strats = self.g.nodes().ids([f.node_id for f in files])\
                                   .path_end(['experimental_strategy']).all()
        exp_strat_summaries = []
        for exp_strat in exp_strats:
            log.info('Query for experimental strategies -> participant')
            participant_count = self.combine_paths_from(
                str(exp_strat.node_id),
                exp_strat_to_part_paths,
                'participant').count()
            log.info('Query for experimental strategies -> files')
            file_count = self.g.nodes().ids(exp_strat.node_id)\
                                       .path_end(['file']).count()
            exp_strat_summaries.append({
                'participant_count': participant_count,
                'file_count': file_count,
                'experimental_strategy': exp_strat['name'],
            })

        # Get data types
        log.info('Query for data_types')
        data_type_summaries = []
        data_types = self.g.nodes().ids([f.node_id for f in files])\
                                   .path_end(file_to_data_type_path).all()
        for data_type in data_types:
            log.info('Query for data type -> participant')
            participant_count = self.combine_paths_from(
                str(data_type.node_id),
                data_type_to_part_paths,
                'participant').count()
            log.info('Query for data type -> file')
            file_count = self.g.nodes().ids([d.node_id for d in data_types])\
                                       .path_end(['data_subtype', 'file'])\
                                       .count()
            data_type_summaries.append({
                'participant_count': participant_count,
                'data_type': data_type['name'],
                'file_count': file_count,
            })

        # Compile summary
        doc['summary'] = {
            'participant_count': len(parts),
            'file_count': len(files),
            'file_size': sum([f['file_size'] for f in files]),
        }
        if exp_strat_summaries:
            doc['summary']['experimental_strategies'] = exp_strat_summaries
        if data_type_summaries:
            doc['summary']['data_types'] = data_type_summaries
        return doc
