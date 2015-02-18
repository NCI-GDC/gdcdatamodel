from gdcdatamodel.mappings import (
    participant_tree, participant_traversal,
    file_tree, file_traversal,
    annotation_tree, annotation_traversal,
    ONE_TO_ONE, ONE_TO_MANY
)
import logging
from cdisutils.log import get_logger
import networkx as nx
from psqlgraph import Edge
from pprint import pprint
import itertools
from sqlalchemy.orm import joinedload
from progressbar import *

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
        self.patch_trees()
        self.ptree_mapping = {'participant': participant_tree.to_dict()}
        self.ftree_mapping = {'file': file_tree.to_dict()}
        self.leaf_nodes = ['center', 'tissue_source_site']
        self.experimental_strategies = {}
        self.data_types = {}
        self.differentiated_edges = [
            ('file', 'member_of', 'archive'),
            ('archive', 'member_of', 'file')
        ]

    def pbar(self, title, maxval):
        pbar = ProgressBar(widgets=[
            title, Percentage(), ' ', Bar(marker='#', left='[',right=']'), ' ',
            ETA(), ' '], maxval=maxval)
        pbar.update(0)
        return pbar

    def es_bulk_upload(self, es, index, doc_type, docs, batch_size=256):
        instruction = {"index": {"_index": index, "_type": doc_type}}
        pbar, results = self.pbar('Batch upload ', len(docs)), []

        def body():
            start = pbar.currval
            for doc in docs[start:start+batch_size]:
                yield instruction
                yield doc
                pbar.update(pbar.currval+1)
        while pbar.currval < len(docs):
            results.append(es.bulk(body=body()))
        pbar.finish()
        return results

    def patch_trees(self):
        file_tree.data_subtype.corr = (ONE_TO_ONE, 'data_subtype')
        participant_tree.file.file.corr = (ONE_TO_MANY, 'files')
        self.ptree_mapping = {'participant': participant_tree.to_dict()}

    def cache_database(self):
        pbar = self.pbar('Caching Database: ', self.g.edges().count())
        for e in self.g.edges().options(joinedload(Edge.src))\
                               .options(joinedload(Edge.dst))\
                               .yield_per(int(1e5)):
            pbar.update(pbar.currval+1)
            needs_differentiation = ((e.src.label, e.label, e.dst.label)
                                     in self.differentiated_edges)
            if needs_differentiation and e.properties:
                self.G.add_edge(
                    e.src, e.dst, label=e.label, props=e.properties)
            elif needs_differentiation and not e.properties:
                self.G.add_edge(e.src, e.dst, label=e.label)
            elif e.properties:
                self.G.add_edge(e.src, e.dst, props=e.properties)
            else:
                self.G.add_edge(e.src, e.dst)
        pbar.finish()
        print('Cached {} nodes'.format(self.G.number_of_nodes()))

    def nodes_labeled(self, label):
        for n, p in self.G.nodes_iter(data=True):
            if n.label == label:
                yield n

    def neighbors_labeled(self, node, labels):
        labels = labels if hasattr(labels, '__iter__') else [labels]
        for n in self.G.neighbors(node):
            if n.label in labels:
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
        if node.label in self.leaf_nodes:
            return {}
        submap = mapping[node.label]
        corr, plural = submap['corr']
        for child in self.G.neighbors(node):
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

    def walk_path(self, node, path, whole=False):
        if path:
            for neighbor in self.neighbors_labeled(node, path[0]):
                if whole or (len(path) == 1 and path[0] == neighbor.label):
                    yield neighbor
                for n in self.walk_path(neighbor, path[1:], whole):
                    yield n

    def walk_paths(self, node, paths, whole=False):
        return {n for n in itertools.chain(
            *[self.walk_path(node, path, whole=whole)
              for path in paths])}

    def cache_data_types(self):
        if len(self.data_types):
            return
        print('Caching data types')
        for data_type in self.nodes_labeled('data_type'):
            self.data_types[data_type] = set(self.walk_path(
                data_type, ['data_subtype', 'file']))

    def cache_experimental_strategies(self):
        if len(self.experimental_strategies):
            return
        print('Caching expertimental strategies')
        for exp_strat in self.nodes_labeled('experimental_strategy'):
            self.experimental_strategies[exp_strat] = set(self.walk_path(
                exp_strat, ['file']))

    def get_exp_strats(self, files):
        self.cache_experimental_strategies()
        for exp_strat, file_list in self.experimental_strategies.iteritems():
            intersection = (file_list & files)
            if intersection:
                yield {'expertimental_strategy': exp_strat['name'],
                       'file_count': len(intersection)}

    def get_data_types(self, files):
        self.cache_data_types()
        for data_type, file_list in self.data_types.iteritems():
            intersection = (file_list & files)
            if intersection:
                yield {'data_type': data_type['name'],
                       'file_count': len(intersection)}

    def get_participant_summary(self, node, files):
        return {
            'data_file_count': len(files),
            'file_size': sum([f['file_size'] for f in files]),
            'expertimental_strategies': list(self.get_exp_strats(files)),
            'data_types': list(self.get_data_types(files)),
        }

    def denormalize_participant(self, node):
        ptree = {node: self.create_tree(node, self.ptree_mapping, {})}
        participant = self.walk_tree(node, ptree, self.ptree_mapping, [])[0]
        files = self.walk_paths(node, participant_traversal['file'])
        participant['summary'] = self.get_participant_summary(node, files)
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
        doc = self._get_base_doc(node)

        # Walk to neighbors
        auto_neighbors = (dict(file_tree).pop('archive')).keys()
        for neighbor in set(self.neighbors_labeled(node, auto_neighbors)):
            corr, label = file_tree[neighbor.label]['corr']
            if corr == ONE_TO_ONE:
                assert label not in doc
                doc[label] = self._get_base_doc(neighbor)
            else:
                if label not in doc:
                    doc[label] = []
                doc[label].append(self._get_base_doc(neighbor))

        # Get archives
        archives, related_archives = [], []
        for archive in set(self.neighbors_labeled(node, 'archive')):
            if self.G[node][archive].get('label') == 'member_of':
                archives.append(self._get_base_doc(archive))
            else:
                related_archives.append(self._get_base_doc(archive))
        if archives:
            doc['archives'] = archives
        if related_archives:
            doc['related_archives'] = related_archives

        # Get data type
        if 'data_subtype' in doc.keys():
            self.cache_data_types()
            for data_type, _files in self.data_types.iteritems():
                if node not in _files:
                    continue
                doc['data_subtype']['data_type'] = self._get_base_doc(data_type)

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

        """
        doc = self._get_base_doc(p)

        # Get programs
        program = self.neighbors_labeled(p, 'program').next()
        print('Program: {}'.format(program))
        doc['program'] = self._get_base_doc(program)

        print('Finding participants')
        parts = list(self.neighbors_labeled(p, 'participant'))
        print('Got {} participants'.format(len(parts)))

        # Construct paths
        paths = [
            ['file'],
            ['sample', 'file'],
            ['sample', 'aliquot', 'file'],
            ['sample', 'portion', 'file'],
            ['sample', 'portion', 'analyte', 'file'],
            ['sample', 'portion', 'analyte', 'aliquot', 'file'],
        ]

        # Get files
        print('Getting files')
        files = set()
        part_files = {}
        for part in parts:
            part_files[part] = self.walk_paths(part, paths)
            files = files.union(part_files[part])
        print('Got {} files from {} participants'.format(
            len(files), len(part_files)))

        # Get data types
        exp_strat_summaries = []
        for exp_strat in self.nodes_labeled('experimental_strategy'):
            print('{} {}'.format(exp_strat, exp_strat['name']))
            dt_files = set(self.walk_path(exp_strat, ['file']))
            if not len(dt_files & files):
                continue
            participant_count = len(
                {p for p, p_files in part_files.iteritems()
                 if len(dt_files & p_files)})
            exp_strat_summaries.append({
                'participant_count': participant_count,
                'experimental_strategy': exp_strat['name'],
                'file_count': len(dt_files),
            })

        # Get data types
        data_type_summaries = []
        for data_type in self.nodes_labeled('data_type'):
            print('{} {}'.format(data_type, data_type['name']))
            dt_files = set(self.walk_path(data_type, ['data_subtype', 'file']))
            if not len(dt_files & files):
                continue
            participant_count = len(
                {p for p, p_files in part_files.iteritems()
                 if len(dt_files & p_files)})
            data_type_summaries.append({
                'participant_count': participant_count,
                'data_type': data_type['name'],
                'file_count': len(dt_files),
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

    def denormalize_participants(self, nodes):
        total_part_docs, total_file_docs = [], []
        pbar = self.pbar('Denormalizing participants ', len(nodes))
        for n in nodes:
            part_doc, file_docs = self.denormalize_participant(n)
            total_part_docs.append(part_doc)
            total_file_docs += file_docs
            pbar.update(pbar.currval+1)
        pbar.finish()
        return total_part_docs, total_file_docs

    def denormalize_projects(self):
        return map(self.denormalize_project, self.nodes_labeled('project'))
