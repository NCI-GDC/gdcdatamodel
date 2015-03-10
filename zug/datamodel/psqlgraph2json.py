from gdcdatamodel.mappings import (
    annotation_tree, participant_tree,
    file_tree, file_traversal,
    ONE_TO_ONE, ONE_TO_MANY
)
import random
import logging
from cdisutils.log import get_logger
import networkx as nx
from psqlgraph import Edge
import itertools
from sqlalchemy.orm import joinedload
from progressbar import ProgressBar, Percentage, Bar, ETA
from copy import copy
import json


log = get_logger("psqlgraph2json")
log.setLevel(level=logging.INFO)


class PsqlGraph2JSON(object):

    """
    """

    def __init__(self, psqlgraph_driver):
        """Walks the graph to produce elasticsearch json documents.

        """
        self.g = psqlgraph_driver
        self.G = nx.Graph()
        self.patch_trees()
        self.leaf_nodes = ['center', 'tissue_source_site']
        self.experimental_strategies = {}
        self.data_types = {}

        # The body of these nested documents will be flattened into
        # the parent document using the given key's value
        self.flatten = {
            'tag': 'name',
            'platform': 'name',
            'data_format': 'name',
            'data_subtype': 'name',
            'experimental_strategy': 'name',
            'data_level': 'name',
        }

        # The edges below will maintain labels in the in memory graph,
        # all others will be discarded
        self.differentiated_edges = [
            ('file', 'member_of', 'archive'),
            ('archive', 'member_of', 'file'),
            ('file', 'describes', 'participant'),
            ('participant', 'describes', 'file')
        ]

        self.part_to_file_paths = [
            ['file'],
            # ['sample', 'file'],
            # ['sample', 'aliquot', 'file'],
            ['sample', 'portion', 'file'],
            # ['sample', 'portion', 'analyte', 'file'],
            ['sample', 'portion', 'analyte', 'aliquot', 'file'],
        ]

    def pbar(self, title, maxval):
        """Create and initialize a custom progressbar

        :param str title: The text of the progress bar
        "param int maxva': The maximumum value of the progress bar

        """
        pbar = ProgressBar(widgets=[
            title, Percentage(), ' ',
            Bar(marker='#', left='[', right=']'), ' ',
            ETA(), ' '], maxval=maxval)
        pbar.update(0)
        return pbar

    def patch_trees(self):
        """This is a hack on top of the source of truth mappings to make the
        trees work with the graph walking code

        """
        # Add leaves to root for things like target
        participant_tree.aliquot = participant_tree.sample\
                                                   .portion\
                                                   .analyte\
                                                   .aliquot

        # Format tree in way that allows uniform walking
        self.ptree_mapping = {'participant': participant_tree.to_dict()}
        self.ftree_mapping = {'file': file_tree.to_dict()}
        self.atree_mapping = {'annotation': annotation_tree.to_dict()}

    def cache_database(self):
        """Load the database into memory and remember only edge labels that we
        will need to distinguish later.

        """
        with self.g.session_scope():
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

    def save_database(self, path):
        node_path = path + '.nodes'
        edge_path = path + '.edges'
        pbar = self.pbar('Saving Nodes: ', self.G.number_of_nodes())
        with open(node_path, 'w') as f:
            for node in self.G.nodes():
                f.write(json.dumps({
                    'node_id': node.node_id,
                    'label': node.label,
                    'properties': node.properties}))
                f.write('\n')
                pbar.update(pbar.currval+1)
        pbar.finish()
        pbar = self.pbar('Saving edges: ', self.G.number_of_nodes())
        with open(edge_path, 'w') as f:
            for edge in self.G.edges():
                src, dst = edge
                edge = {'src_id': src.node_id, 'dst_id': dst.node_id}
                edge.update(self.G[src][dst])
                f.write(json.dumps(edge))
                f.write('\n')
                pbar.update(pbar.currval+1)

    def nodes_labeled(self, label):
        """Returns an iterator over the edges in the graph with label `label`

        """

        for n, p in self.G.nodes_iter(data=True):
            if n.label == label:
                yield n

    def neighbors_labeled(self, node, labels):
        """For a given node, return an iterator with generates neighbors to
        that node that are in a list of labels.  `label` can be either a
        string or list of strings.

        """

        labels = labels if hasattr(labels, '__iter__') else [labels]
        for n in self.G.neighbors(node):
            if n.label in labels:
                yield n

    def parse_tree(self, tree, result):
        """Recursively walk a mapping tree and generate a simpler tree with
        just node labels and not correspondences.

        """

        for key in tree:
            if key != 'corr':
                result[key] = {}
                self.parse_tree(tree[key], result[key])
        return result

    def _get_base_doc(self, node):
        """This is the basic document generator.  Take all the properties of a
        node and add it the the result.  The result doc will have *_id
        where * is the node type.

        """

        base = {'{}_id'.format(node.label): node.node_id}
        base.update(node.properties)
        return base

    def create_tree(self, node, mapping, tree):
        """Recursively walk a mapping to create a walkable tree.

        """

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
        """Recursively walk from a node to all possible neighbors that are
        allowed in the tree structure.  Add the node's properties to the doc.

        """

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
        """Recursively copy the tree so that it can later be pruned per file.

        """

        for node in original:
            new[node] = {}
            self.copy_tree(original[node], new[node])
        return new

    def walk_path(self, node, path, whole=False):
        """Given a list of strings, treat it as a path, and yield the end of
        possible traversals.  If `whole` is true, return every node
        along the traversal.

        """

        if path:
            for neighbor in self.neighbors_labeled(node, path[0]):
                if whole or (len(path) == 1 and path[0] == neighbor.label):
                    yield neighbor
                for n in self.walk_path(neighbor, path[1:], whole):
                    yield n

    def walk_paths(self, node, paths, whole=False):
        """Given a list of paths, yield the result of walking each path. If
        `whole` is true, return every node along each traversal.

        """

        return {n for n in itertools.chain(
            *[self.walk_path(node, path, whole=whole)
              for path in paths])}

    def remove_bam_index_files(self, files):
        return {f for f in files if not f['file_name'].endswith('.bai')}

    def _cache_data_types(self):
        """Looking up the files that are classified in each data_type is a
        common computation.  Here we cache this information for easy retrieval.

        """

        if len(self.data_types):
            return
        print('Caching data types')
        for data_type in self.nodes_labeled('data_type'):
            self.data_types[data_type] = self.remove_bam_index_files(
                set(self.walk_path(data_type, ['data_subtype', 'file'])))

    def _cache_experimental_strategies(self):
        """Looking up the files that are classified in each
        experimental_strategy is a common computation.  Here we cache
        this information for easy retrieval.

        """

        if len(self.experimental_strategies):
            return
        print('Caching experitmental strategies')
        for exp_strat in self.nodes_labeled('experimental_strategy'):
            self.experimental_strategies[exp_strat] = set(self.walk_path(
                exp_strat, ['file']))

    def get_exp_strats(self, files):
        """Get the set of experimental_strategies where intersection of the
        set `files` and the set of files that relate to that
        experimental_strategy is non-null

        """
        self._cache_experimental_strategies()
        for exp_strat, file_list in self.experimental_strategies.iteritems():
            intersection = (file_list & files)
            if intersection:
                yield {'experimental_strategy': exp_strat['name'],
                       'file_count': len(intersection)}

    def get_data_types(self, files):
        """Get the set of data_types where intersection of the
        set `files` and the set of files that relate to that
        data_type is non-null

        """
        self._cache_data_types()
        for data_type, file_list in self.data_types.iteritems():
            intersection = (file_list & files)
            if intersection:
                yield {'data_type': data_type['name'],
                       'file_count': len(intersection)}

    def get_participant_summary(self, node, files):
        """Generate a dictionary containing a summary of a particiants files
        and file classifications

        """
        return {
            'file_count': len(files),
            'file_size': sum([f['file_size'] for f in files]),
            'experimental_strategies': list(self.get_exp_strats(files)),
            'data_types': list(self.get_data_types(files)),
        }

    def reconstruct_biospecimen_paths(self, participant):
        """For each sample.aliquot, reconstruct entire path

        """

        for sample in participant.get('samples', []):
            sample['portions'] = sample.get('portions', [])
            for aliquot in sample.pop('aliquots', []):
                sample['portions'].append({'portion': {'analyte': {aliquot}}})

    def get_metadata_files(self, participant):
        """Return the biospecimen.xml and clinical.xml files that contain a
        participants biospecimen and clinical information.

        """

        neighbors = self.G[participant]
        files = []
        for n in neighbors:
            if self.G[participant][n].get('label', None) == 'describes':
                files.append(self._get_base_doc(n))
        return files

    def patch_project(self, project_doc):
        code = project_doc.pop('code')
        program = project_doc['program']['name']
        project_id = '{}-{}'.format(program, code)
        project_doc['project_id'] = project_id

    def verify_data_type_count(self, participant, data_type):
        calc = len([f for f in participant['files']
                    if f['data_type'] == data_type])
        act = ([d['file_count'] for d in participant['summary']['data_types']
                if d['data_type'] == data_type][:1] or [0])[0]
        assert act == calc, '{}: {} != {}'.format(data_type, act, calc)

    def participant_qa(self, node, participant):
        # Assert file count = summary.file_count
        assert len(participant['files'])\
            == participant['summary']['file_count'],\
            '{}: {} != {}'.format(node.node_id, len(participant['files']),
                                  participant['summary']['file_count'])

    def denormalize_participant(self, node):
        """Given a participant node, return the entire participant document,
        the files belonging to that participant, and the annotations
        that were aggregated to those files.

        """

        # Walk graph naturally for tree of node objects
        ptree = {node: self.create_tree(node, self.ptree_mapping, {})}
        # Use tree to create nested json
        participant = self.walk_tree(node, ptree, self.ptree_mapping, [])[0]
        # Walk from participant to all file leaves
        files = self.remove_bam_index_files(
            self.walk_paths(node, self.part_to_file_paths))

        # Create participant summary
        participant['summary'] = self.get_participant_summary(node, files)

        # Take any out of place nodes and put then in correct place in tree
        self.reconstruct_biospecimen_paths(participant)
        # Get the metadatafiles that generated the participant
        participant['metadata_files'] = self.get_metadata_files(node)

        self.patch_project(participant['project'])
        project = participant['project']

        # Denormalize the participants files
        def get_file(f):
            return self.denormalize_file(f, self.copy_tree(ptree, {}))
        participant['files'] = map(get_file, files)

        # Add properties to all annotations
        for a in [a for f in participant['files']
                  for a in f.get('annotations', [])]:
            a['participant_id'] = node.node_id

        # Create copy of annotations and add properties
        annotations = [copy(a) for f in participant['files']
                       for a in f.get('annotations', [])]
        for a in annotations:
            a['project'] = project

        # Check for obvious errors
        # self.participant_qa(node, participant)

        return participant, participant['files'], annotations

    def prune_participant(self, relevant_nodes, ptree, keys):
        """Start with whole participant tree and remove any nodes that did not
        contribute the the creation of this file.

        """
        for node in ptree.keys():
            if ptree[node]:
                self.prune_participant(relevant_nodes, ptree[node], keys)
            if node.label in keys and node not in relevant_nodes:
                ptree.pop(node)

    def add_file_neighbors(self, node, doc):
        """Given a file, walk to all of it's neighbors specified by the schema
        and add them to the document.

        """

        auto_neighbors = [n for n in dict(file_tree).keys()
                          if n not in ['archive']]
        for neighbor in set(self.neighbors_labeled(node, auto_neighbors)):
            corr, label = file_tree[neighbor.label]['corr']
            if neighbor.label in self.flatten:
                base = neighbor[self.flatten[neighbor.label]]
            else:
                base = self._get_base_doc(neighbor)
            if corr == ONE_TO_ONE:
                assert label not in doc
                doc[label] = base
            else:
                if label not in doc:
                    doc[label] = []
                doc[label].append(base)

    def patch_file_datetimes(self, doc):
        doc['published_datetime'] = None
        doc['uploaded_datetime'] = 1425340539

    def add_related_files(self, node, doc):
        """Given a file, walk to any neighboring files and add them to the
        related_files section of the document.

        """

        # Get related_files
        related_files = list(self.neighbors_labeled(node, 'file'))
        rf_docs = []
        for related_file in related_files:
            rf_doc = self._get_base_doc(related_file)
            for dst in self.neighbors_labeled(related_file, 'data_subtype'):
                rf_doc['data_subtype'] = dst['name']
                self.add_data_type(related_file, rf_doc)
            self.patch_file_datetimes(rf_doc)
            if related_file['file_name'].endswith('.bai'):
                rf_doc['type'] = 'bai'
            else:
                rf_doc['type'] = None
            rf_docs.append(rf_doc)

        for archive in set(self.neighbors_labeled(node, 'archive')):
            if self.G[node][archive].get('label') != 'member_of':
                name = '{}.{}.0'.format(
                    archive['submitter_id'], archive['revision'])
                rf_docs.append({
                    'file_id': archive.node_id,
                    'file_name': name,
                    'type': 'magetab'
                })

        if rf_docs:
            doc['related_files'] = rf_docs

    def add_archives(self, node, doc):
        """For each archive attached to a given file node, multixplex on
        whether it is a containing or related archive and add it to the
        respective places in the doc.

        """

        for archive in set(self.neighbors_labeled(node, 'archive')):
            if self.G[node][archive].get('label') == 'member_of':
                doc['archive'] = self._get_base_doc(archive)

    def add_data_type(self, node, doc):
        """Add the data_subtype to the file document with child data_type

        """

        self._cache_data_types()
        data_types = [dt['name'] for dt, _files in self.data_types.items()
                      if node in _files]
        if data_types:
            doc['data_type'] = data_types[0]

    def add_participants(self, node, ptree, doc):
        """Given a file and a participant tree, re-insert the participant as a
        child of file with only the biospecimen entities that are
        direct ancestors of the file.

        """

        relevant = self.walk_paths(node, file_traversal.participant, True)
        self.prune_participant(relevant, ptree, [
            'sample', 'portion', 'analyte', 'aliquot', 'file'])
        doc['participants'] = map(
            lambda p: self.walk_tree(p, ptree, self.ptree_mapping, [])[0],
            ptree)
        for p in doc['participants']:
            self.patch_project(p['project'])
        return relevant

    def add_annotations(self, node, relevant, doc):
        """Given a file node, aggregate all of the annotations from a pruned
        participant tree and insert them at the root level of the file
        document.

        """

        annotations = doc.pop('annotations', [])
        for parent in relevant:
            for p_annotation in self.neighbors_labeled(parent, 'annotation'):
                annotations.append(self.denormalize_annotation(p_annotation))
        if annotations:
            doc['annotations'] = annotations

    def add_acl(self, node, doc):
        """Add the protection status of a file to the file document.

        """

        if node.acl == ['open']:
            doc['access'] = 'open'
        else:
            doc['access'] = 'protected'
        doc['acl'] = node.acl

    def add_file_derived_from_entities(self, node, doc, participant_id):
        entities = self.neighbors_labeled(node, ['participant', 'sample',
                                                 'portion', 'slide', 'analyte',
                                                 'aliquot'])
        doc['associated_entities'] = [{
            'entity_type': e.label,
            'entity_id': e.node_id,
            'participant_id': participant_id,
        } for e in entities]

    def denormalize_file(self, node, ptree):
        """Given a participants tree and a file node, create the file json
        document.

        """
        participant_id = ptree.keys()[0].node_id if ptree.keys() else None
        doc = self._get_base_doc(node)
        self.patch_file_datetimes(doc)
        self.add_file_neighbors(node, doc)
        self.add_data_type(node, doc)
        self.add_related_files(node, doc)
        self.add_archives(node, doc)
        relevant = self.add_participants(node, ptree, doc)
        self.add_file_derived_from_entities(node, doc, participant_id)
        self.add_annotations(node, relevant, doc)
        self.add_acl(node, doc)
        return doc

    def denormalize_project(self, p):
        """Summarize a project.

        """
        doc = self._get_base_doc(p)

        # Get programs
        program = self.neighbors_labeled(p, 'program').next()
        log.debug('Program: {}'.format(program))
        doc['program'] = self._get_base_doc(program)

        # project_id <- program.name-project.code
        self.patch_project(doc)

        log.debug('Finding participants')
        parts = list(self.neighbors_labeled(p, 'participant'))
        log.debug('Got {} participants'.format(len(parts)))

        # Get files
        log.debug('Getting files')
        files = set()
        part_files = {}
        for part in parts:
            part_files[part] = self.remove_bam_index_files(
                self.walk_paths(part, self.part_to_file_paths))
            files = files.union(part_files[part])
        log.debug('Got {} files from {} participants'.format(
            len(files), len(part_files)))

        # Get data types
        exp_strat_summaries = []
        self._cache_experimental_strategies()
        for exp_strat in self.nodes_labeled('experimental_strategy'):
            log.debug('{} {}'.format(exp_strat, exp_strat['name']))
            exp_files = (self.experimental_strategies[exp_strat] & files)
            if not len(exp_files):
                continue
            participant_count = len(
                {p for p, p_files in part_files.iteritems()
                 if len(exp_files & p_files)})
            exp_strat_summaries.append({
                'participant_count': participant_count,
                'experimental_strategy': exp_strat['name'],
                'file_count': len(exp_files),
            })

        # Get data types
        data_type_summaries = []
        self._cache_data_types()
        for data_type in self.nodes_labeled('data_type'):
            log.debug('{} {}'.format(data_type, data_type['name']))
            dt_files = (self.data_types[data_type] & files)
            if not len(dt_files):
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

    def upsert_file_into_dict(self, files, f):
        did = f['file_id']
        if did not in files:
            files[did] = f
        else:
            files[did]['participants'] += f['participants']

    def denormalize_participants(self, participants=None):
        """If participants is not specified, denormalize all participants in
        the graph.  If participants is specified, denormalize only those
        given.

        :returns:
            Tuple containing (participant docs, file docs, annotation docs)

        """

        total_part_docs, total_ann_docs = [], []
        if not participants:
            participants = list(self.nodes_labeled('participant'))
        pbar = self.pbar('Denormalizing participants ', len(participants))
        files = {}
        for n in participants:
            part_doc, file_docs, ann_docs = self.denormalize_participant(n)
            total_part_docs.append(part_doc)
            total_ann_docs += ann_docs
            for f in file_docs:
                self.upsert_file_into_dict(files, f)
            pbar.update(pbar.currval+1)
        pbar.finish()
        return total_part_docs, files.values(), total_ann_docs

    def denormalize_projects(self, projects=None):
        """If projects is not specified, denormalize all projects in
        the graph.  If projects is specified, denormalize only those
        given.

        """
        if not projects:
            projects = list(self.nodes_labeled('project'))
        project_docs = []
        pbar = self.pbar('Denormalizing projects ', len(projects))
        for project in projects:
            project_docs.append(self.denormalize_project(project))
            pbar.update(pbar.currval+1)
        pbar.finish()
        return project_docs

    def denormalize_annotation(self, node):
        """Denormalize a specific annotation.

        .. note: The project of an annotation will be injected during
        participant denormalization.

        """
        ann_doc = self._get_base_doc(node)
        items = self.G.neighbors(node)
        assert len(items) == 1
        ann_doc['entity_type'] = items[0].label
        ann_doc['entity_id'] = items[0].node_id
        return ann_doc

    def denormalize_all(self):
        """Return an entire index worth of participant, file, annotation, and
        project documents

        """
        parts, files, annotations = self.denormalize_participants()
        projects = self.denormalize_projects()
        return parts, files, annotations, projects

    def denormalize_sample_parts(self, k=10):
        """Return an entire index worth of participant, file, annotation
         documents

        """
        parts = random.sample(list(self.nodes_labeled('participant')), k)
        parts, files, annotations = self.denormalize_participants(parts)
        return parts, files, annotations

    def denormalize_sample(self, k=10):
        """Return an entire index worth of participant, file, annotation, and
        project documents

        """
        parts, files, annotations = self.denormalize_sample_parts(k)
        projs = random.sample(list(self.nodes_labeled('project')), 1)
        projects = self.denormalize_projects(projs)
        return parts, files, annotations, projects

    def validate_docs(self, part_docs, file_docs, ann_docs, project_docs):
        pass
