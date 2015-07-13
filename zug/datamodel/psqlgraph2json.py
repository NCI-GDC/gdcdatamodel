from gdcdatamodel.mappings import (
    annotation_tree, case_tree,
    file_tree, ONE_TO_ONE, ONE_TO_MANY,
    get_case_es_mapping, get_file_es_mapping,
    get_project_es_mapping, get_annotation_es_mapping,
    TOP_LEVEL_IDS,
)
from zug.datamodel.prelude import DATA_TYPES
import random
import logging
from cdisutils.log import get_logger
import networkx as nx
import itertools
from progressbar import ProgressBar, Percentage, Bar, ETA
from copy import copy, deepcopy
from collections import defaultdict
from math import ceil

log = get_logger("psqlgraph2json")
log.setLevel(level=logging.INFO)


class PsqlGraph2JSON(object):

    """This class handles all of the JSON production for the GDC
    portal. Currently, the entire postgresql database is cached to
    memory.  To save space, edge labels are only maintained if we need
    to distinguish between two different types of edges between a
    single pair of node types.

    Currently, the entire batch of JSON documents is produced at once
    for reasons that follow. There are two topmost denormalization
    functions that are called, denormalize_cases() and
    denormalize_projects(). The former produces all of the
    case, file, and annotations documents. The latter produces
    the project summaries.

    The case denormalization takes the case tree from
    gdcdatamodel and, starting at a case, walks recursively to
    all possible children.  Each child's properties are added to the
    case document at the appropriate level depending on the
    correlation (one to one=singleton, or one to many=list).  The leaf
    node for most paths from case are files, which have a
    special denormalization.

    When a file is gathered from walking the case path, a deep
    copy is both added to the cases file list returned for
    later collection.  Denormalizing a case produces a list of
    files and annotations. Each file is upserted into a persisting
    list of files.  If after denormalizing case 1 who produced
    file A, the upsert involves adding to A the list if not present.
    If we have already gotten file A from another case, it
    means that the file came from multiple cases and we have to
    update file A to also reference case 1.

    In order to make decrease the processing time, there are a lot of
    caching initiatives.  The paths from cases to files are
    cached. The set of files using each data type and experimental
    strategy are cached.  There is also a caching scheme for
    remembering which nodes are walked through a lot and remembering
    which neighbors they have with a given label.

    NOTE: An attempt was made to do this whole thing in parallel,
    however the memory footprint grew to large.  The best method for
    doing this is to use the main process as a workload distributer,
    and have child processes denormalizing cases.  This way,
    the main thread can upsert files on an outbound queue from child
    processes.

    - Josh (jsmiller@uchicago.edu)

    TODOS:
      - figure out a way to parallelize without excess copies

    """

    def __init__(self, psqlgraph_driver):
        """Walks the graph to produce elasticsearch json documents.

        """
        self.g = psqlgraph_driver
        self.G = nx.Graph()
        self.ptree_mapping = {'case': case_tree.to_dict()}
        self.ftree_mapping = {'file': file_tree.to_dict()}
        self.atree_mapping = {'annotation': annotation_tree.to_dict()}
        self.leaf_nodes = ['center', 'tissue_source_site']
        self.experimental_strategies = {}
        self.data_types = {}
        self.popular_nodes = {}
        self.cases = None
        self.projects = None
        self.relevant_nodes = None
        self.annotations = None
        self.annotation_entities = None
        self.entity_cases = None

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
            ('file', 'describes', 'case'),
            ('case', 'describes', 'file'),
        ]

        self.case_to_file_paths = [
            ['file'],
            ['sample', 'aliquot', 'file'],
            ['sample', 'portion', 'file'],
            ['sample', 'portion', 'analyte', 'aliquot', 'file'],
        ]

        self.possible_associated_entites = [
            'portion',
            'aliquot',
            'case',
        ]

        self.file_to_case_paths = [
            list(reversed(l))[1:]+['case']
            for l in self.case_to_file_paths
        ]

    def pbar(self, title, maxval):
        """Create and initialize a custom progressbar

        :param str title: The text of the progress bar
        :param int maxval: The maximumum value of the progress bar

        """
        pbar = ProgressBar(widgets=[
            title, Percentage(), ' ',
            Bar(marker='#', left='[', right=']'), ' ',
            ETA(), ' '], maxval=maxval)
        pbar.update(0)
        return pbar

    ###################################################################
    #                        Tree functions
    ###################################################################

    def parse_tree(self, tree, result):
        """Recursively walk a mapping tree and generate a simpler tree with
        just node labels and not correspondences.

        """

        for key in tree:
            if key != 'corr':
                result[key] = {}
                self.parse_tree(tree[key], result[key])
        return result

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

    def walk_tree(self, node, tree, mapping, doc, level=0,
                  ids=None):
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
                           subdoc[child_plural], level+1, ids=ids)

            # Aggregate ids as we walk the tree
            if ids is not None and child.label in TOP_LEVEL_IDS:
                ids['{}_ids'.format(child.label)].append(child.node_id)
                if child.props.get('submitter_id'):
                    ids['submitter_{}_ids'.format(child.label)].append(
                        child.props.get('submitter_id'))

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

    def _get_base_doc(self, node):
        """This is the basic document generator.  Take all the properties of a
        node and add it the the result.  The result doc will have *_id
        where * is the node type.

        """

        base = {'{}_id'.format(node.label): node.node_id}
        base.update(node.properties)
        return base

    ###################################################################
    #                        Path functions
    ###################################################################

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

    def patch_tcga_ages(self, case):
        """Because TCGA reports ages in years, and target in days, we
        normalize TCGA age_at_diagnosis fields to be
        int(ceil(d*365.25)) where d is the age in days

        """
        program_name = case['project']['program']['name']
        if program_name == 'TCGA':
            clinical = case.get('clinical', {})
            age_in_years = clinical.get('age_at_diagnosis')
            if age_in_years:
                age_in_days = int(ceil(age_in_years*365.25))
                case['clinical']['age_at_diagnosis'] = age_in_days

    ###################################################################
    #                          Cases
    ##################################################################

    def denormalize_case(self, node):
        """Given a case node, return the entire case document,
        the files belonging to that case, and the annotations
        that were aggregated to those files.

        """

        # Walk graph naturally for tree of node objects
        ptree = {node: self.create_tree(node, self.ptree_mapping, {})}

        # Use tree to create nested json
        visited_ids = defaultdict(list)
        case = self.walk_tree(
            node, ptree, self.ptree_mapping, [], ids=visited_ids)[0]

        # Inject a dictionary of ids for each visited entity (in TOP_LEVEL_IDS)
        case.update(visited_ids)

        # Walk from case to all file leaves
        files = self.remove_bam_index_files(
            self.walk_paths(node, self.case_to_file_paths))

        # Create case summary
        case['summary'] = self.get_case_summary(node, files)

        # Take any out of place nodes and put then in correct place in tree
        self.reconstruct_biospecimen_paths(case)
        # Get the metadatafiles that generated the case
        case['metadata_files'] = self.get_metadata_files(node)

        self.patch_project(case['project'])
        project = case['project']

        # Denormalize the cases files
        def get_file(f):
            return self.denormalize_file(f, ptree)
        case['files'] = map(get_file, files)

        # TODO move this logic to project level normalization? It was
        # requested to do this transformation in the es build and not
        # in the data itself, so it is here for now.
        self.patch_tcga_ages(case)

        # Add properties to all annotations
        for a in [a for f in case['files']
                  for a in f.get('annotations', [])]:
            a['case_id'] = node.node_id

        # Create a flattened copy of visited_ids to filter relevant
        # annotations by entity id
        relevant_ids = [eid for etype in visited_ids.itervalues()
                        for eid in etype] + [node.node_id]

        # Create copy of annotations and add properties
        annotations = {a['annotation_id']: copy(a)
                       for f in case['files']
                       for a in f.get('annotations', [])
                       if a['entity_id'] in relevant_ids}
        for a in annotations.itervalues():
            a['project'] = project
            a['case_id'] = node.node_id

        # Copy the files with all cases
        files = deepcopy(case['files'])

        # Trim other cases from fiels
        for f in case['files']:
            f['cases'] = [p for p in f['cases']
                                 if p['case_id'] == node.node_id]
            f.pop('annotations', None)
            f.pop('associated_entities', None)

        self.validate_case(node, case)

        return case, files, annotations.values()

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

    def get_case_summary(self, node, files):
        """Generate a dictionary containing a summary of a cases files
        and file classifications

        """
        return {
            'file_count': len(files),
            'file_size': sum([f['file_size'] for f in files]),
            'experimental_strategies': list(self.get_exp_strats(files)),
            'data_types': list(self.get_data_types(files)),
        }

    def reconstruct_biospecimen_paths(self, case):
        """For each sample.aliquot, reconstruct entire path

        """

        samples = case.get('samples', [])
        correct_aliquots = set()
        for sample in samples:
            for portion in sample.get('portions', []):
                for analyte in portion.get('analytes', []):
                    for aliquot in analyte.get('aliquots', []):
                        correct_aliquots.add(aliquot['aliquot_id'])

        for sample in samples:
            sample['portions'] = sample.get('portions', [])

            # Get all aliquots connected to samples
            sample_aliquots = sample.pop('aliquots', [])
            for aliquot in sample_aliquots:
                if aliquot['aliquot_id'] not in correct_aliquots:
                    sample['portions'].append({
                        'analytes': [{
                            'aliquots': [aliquot]
                        }]})

            for portion in sample['portions']:
                portion['analytes'] = portion.get('analytes', [])

                # Get aliquots connected to portions
                portion_aliquots = portion.pop('aliquots', [])
                for aliquot in portion_aliquots:
                    # Put aliquot under analyte
                    if aliquot['aliquot_id'] not in correct_aliquots:
                        portion['analytes'].append([{
                            'aliquots': [aliquot]}])

    def get_metadata_files(self, case):
        """Return the biospecimen.xml and clinical.xml files that contain a
        cases biospecimen and clinical information.

        """

        neighbors = self.G[case]
        files = []
        for n in neighbors:
            if self.G[case][n].get('label', None) == 'describes':
                files.append(self._get_base_doc(n))
        return files

    def patch_project(self, project_doc):
        code = project_doc.pop('code')
        program = project_doc['program']['name']
        project_id = '{}-{}'.format(program, code)
        project_doc['project_id'] = project_id

    ###################################################################
    #                       File denormalization
    ###################################################################

    def denormalize_file(self, node, ptree):
        """Given a cases tree and a file node, create the file json
        document.

        """

        # Create a copy to avoid mutation of passed argument
        ptree = self.copy_tree(ptree, {})

        # Create base file doc
        case_id = ptree.keys()[0].node_id if ptree.keys() else None
        doc = self._get_base_doc(node)

        # Add file fields
        self.patch_file_datetimes(doc)
        self.add_file_origin(node, doc)
        self.add_file_neighbors(node, doc)
        self.add_data_type(node, doc)
        self.add_related_files(node, doc)
        self.add_archives(node, doc)
        doc['cases'] = []
        relevant = self.add_cases(node, ptree, doc)
        self.add_file_derived_from_entities(node, doc, case_id)
        self.add_annotations(node, relevant, doc)
        self.add_acl(node, doc)

        return doc

    def prune_case(self, relevant_nodes, ptree, keys):
        """Start with whole case tree and remove any nodes that did not
        contribute the the creation of this file.

        """
        for node in ptree.keys():
            if ptree[node]:
                self.prune_case(relevant_nodes, ptree[node], keys)
            if node.label in keys and node not in relevant_nodes:
                ptree.pop(node)

    def add_file_neighbors(self, node, doc):
        """Given a file, walk to all of it's neighbors specified by the schema
        and add them to the document.

        """

        auto_neighbors = [n for n in dict(file_tree).keys()
                          if n not in ['archive', 'portion']]
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
            elif related_file['file_name'].endswith('.sdrf.txt'):
                rf_doc['type'] = 'magetab'
            else:
                rf_doc['type'] = None
            if related_file.acl == ["open"]:
                rf_doc['access'] = 'open'
            else:
                rf_doc['access'] = 'controlled'
            rf_docs.append(rf_doc)

        for archive in set(self.neighbors_labeled(node, 'archive')):
            if self.G[node][archive].get('label') != 'member_of':
                name = '{}.{}.0.tar.gz'.format(
                    archive['submitter_id'], archive['revision'])
                rf_docs.append({
                    'file_id': archive.node_id,
                    'file_name': name,
                    'type': 'magetab',
                    'access': 'open',
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

    def add_cases(self, node, ptree, doc):
        """Given a file and a case tree, re-insert the case as a
        child of file with only the biospecimen entities that are
        direct ancestors of the file.

        """

        relevant = self.relevant_nodes[node]
        self.prune_case(relevant, ptree, [
            'sample', 'portion', 'analyte', 'aliquot', 'file'])
        doc['cases'] = map(
            lambda p: self.walk_tree(p, ptree, self.ptree_mapping, [])[0],
            ptree)
        for p in doc['cases']:
            self.patch_project(p['project'])
            self.reconstruct_biospecimen_paths(p)
        return relevant

    def add_annotations(self, node, relevant, doc):
        """Given a file node, aggregate all of the annotations from a pruned
        case tree and insert them at the root level of the file
        document.

        """

        annotations = doc.pop('annotations', [])
        for r in relevant:
            for a_doc in self.annotation_entities.get(r, {}).itervalues():
                annotations.append(a_doc)
        if annotations:
            doc['annotations'] = annotations

    def add_acl(self, node, doc):
        """Add the protection status of a file to the file document.

        """

        if node.acl == ['open']:
            doc['access'] = 'open'
        else:
            doc['access'] = 'controlled'
        doc['acl'] = node.acl

    def add_file_derived_from_entities(self, node, doc, case_id):
        self._cache_entity_cases()
        entities = self.neighbors_labeled(
            node, self.possible_associated_entites)
        docs = []
        for e in entities:
            case = self.entity_cases[e]
            docs.append({
                'entity_type': e.label,
                'entity_id': e.node_id,
                'case_id': case.node_id,
            })
        if docs:
            doc['associated_entities'] = docs

    def add_file_origin(self, node, doc):
        doc['origin'] = 'migrated'

    def upsert_file_into_dict(self, files, file_doc):
        did = file_doc['file_id']
        if did not in files:
            files[did] = file_doc
        else:
            for case in file_doc['cases']:
                case_id = case['case_id']
                existing_ids = {
                    p['case_id'] for p in files[did]['cases']}
                if case_id not in existing_ids:
                    files[did]['cases'] += file_doc['cases']

    ###################################################################
    #                       Project summaries
    ###################################################################

    def denormalize_project(self, p):
        """Summarize a project.

        """
        self._cache_all()
        doc = self._get_base_doc(p)

        # Get programs
        program = self.neighbors_labeled(p, 'program').next()
        log.info('Program: {}'.format(program))
        doc['program'] = self._get_base_doc(program)

        # project_id <- program.name-project.code
        self.patch_project(doc)

        log.info('Finding cases')
        cases = list(self.neighbors_labeled(p, 'case'))
        log.info('Got {} cases'.format(len(cases)))

        # Get files
        log.info('Getting files')
        files = set()
        case_files = {}
        for case in cases:
            case_files[case] = self.remove_bam_index_files(
                self.walk_paths(case, self.case_to_file_paths))
            files = files.union(case_files[case])
        log.info('Got {} files from {} cases'.format(
            len(files), len(case_files)))

        # Get data types
        exp_strat_summaries = []
        self._cache_experimental_strategies()
        for exp_strat in self.experimental_strategies.keys():
            log.info('{} {}'.format(exp_strat, exp_strat['name']))
            exp_files = (self.experimental_strategies[exp_strat] & files)
            if not len(exp_files):
                continue
            case_count = len(
                {p for p, p_files in case_files.iteritems()
                 if len(exp_files & p_files)})
            exp_strat_summaries.append({
                'case_count': case_count,
                'experimental_strategy': exp_strat['name'],
                'file_count': len(exp_files),
            })

        # Get data types
        data_type_summaries = []
        self._cache_data_types()
        for data_type in self.data_types.keys():
            log.info('{} {}'.format(data_type, data_type['name']))
            dt_files = (self.data_types[data_type] & files)
            if not len(dt_files):
                continue
            case_count = len(
                {p for p, p_files in case_files.iteritems()
                 if len(dt_files & p_files)})
            data_type_summaries.append({
                'case_count': case_count,
                'data_type': data_type['name'],
                'file_count': len(dt_files),
            })

        # Compile summary
        doc['summary'] = {
            'case_count': len(cases),
            'file_count': len(files),
            'file_size': sum([f['file_size'] for f in files]),
        }
        if exp_strat_summaries:
            doc['summary']['experimental_strategies'] = exp_strat_summaries
        if data_type_summaries:
            doc['summary']['data_types'] = data_type_summaries
        return doc

    ###################################################################
    #                     Topmost denorm functions
    ###################################################################

    def denormalize_cases(self, cases=None):
        """If cases is not specified, denormalize all cases in
        the graph.  If cases is specified, denormalize only those
        given.

        :returns:
            Tuple containing (case docs, file docs, annotation docs)

        """

        self._cache_all()
        case_docs, ann_docs, file_docs = [], {}, {}
        if not cases:
            cases = self.cases
        pbar = self.pbar('Denormalizing cases ', len(cases))
        for n in cases:
            pa, fi, an = self.denormalize_case(n)
            case_docs.append(pa)
            for a in an:
                if a['annotation_id'] not in ann_docs:
                    ann_docs[a['annotation_id']] = a
            for f in fi:
                self.upsert_file_into_dict(file_docs, f)
            pbar.update(pbar.currval+1)
        pbar.finish()
        return case_docs, file_docs.values(), ann_docs.values()

    def denormalize_projects(self, projects=None):
        """If projects is not specified, denormalize all projects in
        the graph.  If projects is specified, denormalize only those
        given.

        """

        self._cache_all()
        if not projects:
            projects = self.projects
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
        case denormalization.

        """
        ann_doc = self._get_base_doc(node)
        entities = self.G.neighbors(node)
        assert len(entities) == 1, 'Annotation has multiple entities'
        entity = entities[0]
        ann_doc['entity_type'] = entity.label
        ann_doc['entity_id'] = entity.node_id
        if 'submitter_id' in entity.properties:
            ann_doc['entity_submitter_id'] = entity['submitter_id']
        return ann_doc

    def denormalize_all(self):
        """Return an entire index worth of case, file, annotation, and
        project documents

        """
        cases, files, annotations = self.denormalize_cases()
        projects = self.denormalize_projects()
        return cases, files, annotations, projects

    def denormalize_cases_sample(self, k=10):
        """Return an entire index worth of case, file, annotation
         documents

        """
        self._cache_all()
        cases = random.sample(self.cases, k)
        cases, files, annotations = self.denormalize_cases(cases)
        return cases, files, annotations

    def denormalize_sample(self, k=10):
        """Return an entire index worth of case, file, annotation, and
        project documents

        """
        cases, files, annotations = self.denormalize_sample_cases(k)
        projs = random.sample(self.projects, 1)
        projects = self.denormalize_projects(projs)
        return cases, files, annotations, projects

    ###################################################################
    #                         Graph functions
    ###################################################################

    def nodes_labeled(self, labels):
        """Returns an iterator over the edges in the graph with label `label`

        """

        labels = tuple(labels) if hasattr(labels, '__iter__') else (labels,)
        for n, p in self.G.nodes_iter(data=True):
            if n.label in labels:
                yield n

    def neighbors_labeled(self, node, labels):
        """For a given node, return an iterator with generates neighbors to
        that node that are in a list of labels.  `label` can be either a
        string or list of strings.

        """
        labels = tuple(labels) if hasattr(labels, '__iter__') else (labels,)

        if node in self.popular_nodes:
            if labels not in self.popular_nodes[node]:
                neighbors = self._cache_popular_neighbor(
                    node, self.G.neighbors(node), labels)
            else:
                neighbors = self.popular_nodes[node][labels]
        else:
            temp = self.G.neighbors(node)
            if len(temp) > 200:
                neighbors = self._cache_popular_neighbor(node, temp, labels)
            else:
                neighbors = {n for n in temp if n.label in labels}

        for n in neighbors:
            yield n

    ###################################################################
    #                       Validation functions
    ###################################################################

    def validate_project_file_counts(self, project_doc, file_docs):
        print 'Validating {}'.format(project_doc['project_id'])
        actual = len([f for f in file_docs
                      if project_doc['project_id']
                      in {p['project']['project_id']
                          for p in f['cases']}])
        expected = project_doc['summary']['file_count']
        assert actual == expected, '{} file count mismatch: {} != {}'.format(
            project_doc['project_id'], actual, expected)

    def validate_docs(self, case_docs, file_docs, ann_docs, project_docs):
        for project_doc in project_docs:
            self.validate_project_file_counts(project_doc, file_docs)
            case_sample = random.sample(case_docs, min(len(case_docs), 100))
            for case_doc in case_sample:
                self.verify_data_type_count(case_doc)
        self.validate_annotations(ann_docs)

    def validate_annotations(self, ann_docs):
        for ann_doc in ann_docs:
            if ann_doc['entity_type'] == 'case':
                assert ann_doc['entity_id'] == ann_doc['case_id']

    def verify_data_type_count(self, case):
        for data_type in DATA_TYPES.keys():
            calc = len([f for f in case['files']
                        if f.get('data_type') == data_type])
            act = ([d['file_count'] for d in case['summary']['data_types']
                    if d['data_type'] == data_type][:1] or [0])[0]
            assert act == calc, '{}: {} != {}'.format(data_type, act, calc)

    def validate_against_mapping(self, doc, mapping):
        """Recursively verify that all keys in the document are in the
        provided Elasticsearch mapping

        """
        if isinstance(doc, dict):
            # Recurse through all keys in dictionary
            for doc_key, child in doc.iteritems():
                assert doc_key in mapping['properties'].keys(),\
                    "Key '{}' was not found in mapping keys {}".format(
                        doc_key, mapping['properties'].keys())
                self.validate_against_mapping(
                    child, mapping['properties'][doc_key])
        elif isinstance(doc, list):
            # Loop over all all items in the list. Note that ES
            # mappings do not distinguish between lists of subdocs and
            # single subdocs.
            for list_entry in doc:
                self.validate_against_mapping(list_entry, mapping)

    def validate_case(self, node, case):
        # Assert file count = summary.file_count
        assert len(case['files'])\
            == case['summary']['file_count'],\
            '{}: {} != {}'.format(node.node_id, len(case['files']),
                                  case['summary']['file_count'])

        # Check for keys that are in the doc but not in the mapping
        self.validate_against_mapping(
            case, get_case_es_mapping())

    ###################################################################
    #                       Caching functions
    ###################################################################

    def is_file_indexed(self, node):
        if node.system_annotations.get("to_delete"):
            return False
        if node.label == 'file' and not node.state == 'live':
            return False
        return True

    def is_edge_indexed(self, edge):
        if not (self.is_file_indexed(edge.src) and
                self.is_file_indexed(edge.dst)):
            return False
        if (edge.src.label == 'file' and
           edge.label == 'data_from' and
           edge.dst.label == 'file'):
            return False
        return True

    def cache_database(self):
        """Load the database into memory and remember only edge labels that we
        will need to distinguish later.

        """
        with self.g.session_scope():
            pbar = self.pbar('Caching Database: ', self.g.edges().count())
            for e in self.g.edges().yield_per(int(1e5)):
                pbar.update(pbar.currval+1)
                needs_differentiation = ((e.src.label, e.label, e.dst.label)
                                         in self.differentiated_edges)
                if not self.is_edge_indexed(e):
                    continue
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
        self._cache_all()
        print('Cached {} nodes'.format(self.G.number_of_nodes()))

    def _cache_all(self):
        """Create key value maps to cache nodes by label, by path, etc.

        """

        self._cache_experimental_strategies()
        self._cache_data_types()
        self._cache_annotations()
        self._cache_relevant_nodes()
        self._cache_entity_cases()
        self._cache_cases()
        self._cache_projects()

    def _cache_projects(self):
        if not self.projects:
            print 'Caching projects...'
            self.projects = list(self.nodes_labeled('project'))

    def _cache_cases(self):
        if not self.cases:
            print 'Caching cases...'
            self.cases = list(self.nodes_labeled('case'))

    def _cache_entity_cases(self):
        if self.entity_cases:
            return
        entities = list(self.nodes_labeled(self.possible_associated_entites))
        pbar = self.pbar('Caching entity cases: ', len(entities))
        self.entity_cases = {}
        for e in entities:
            paths = [p[1:] for p in self.file_to_case_paths if p[0] == e.label]
            cases = self.walk_paths(e, paths)
            assert len(cases) == 1,\
                '{}: Found {} cases: {}'.format(
                    e, len(cases), paths)
            self.entity_cases[e] = cases.pop()
            pbar.update(pbar.currval+1)
        pbar.finish()

    def _cache_relevant_nodes(self):
        if self.relevant_nodes:
            return
        files = list(self.nodes_labeled('file'))
        self.relevant_nodes = {}
        pbar = self.pbar('Caching file paths: ', len(files))
        for f in files:
            self.relevant_nodes[f] = self.walk_paths(
                f, self.file_to_case_paths, whole=True)
            pbar.update(pbar.currval+1)
        pbar.finish()

    def _cache_annotations(self):
        if not self.annotations:
            # cache what nodes are annotations
            self.annotations = list(self.nodes_labeled('annotation'))
        if self.annotation_entities:
            # we've already cached the related entities
            return
        if not self.annotations:
            # there aren't any entities to relate
            self.annotation_entities = {}
            log.warn('No annotations found in the cached database!')
            return
        pbar = self.pbar('Caching annotations: ', len(self.annotations))
        self.annotation_entities = {}
        for a in self.annotations:
            for n in self.G.neighbors(a):
                if n not in self.annotation_entities:
                    self.annotation_entities[n] = {}
                a_doc = self.denormalize_annotation(a)
                self.annotation_entities[n][a.node_id] = a_doc
            pbar.update(pbar.currval+1)
        pbar.finish()

    def _cache_popular_neighbor(self, node, neighbors, labels):
        if node not in self.popular_nodes:
            self.popular_nodes[node] = {}
        self.popular_nodes[node][labels] = {
            n for n in neighbors if n.label in labels}
        return self.popular_nodes[node][labels]

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
