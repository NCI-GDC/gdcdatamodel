import unittest
import os
from gdcdatamodel import get_case_es_mapping
from gdcdatamodel.models import File, Aliquot, Case, Annotation, Project
from zug.datamodel.psqlgraph2json import PsqlGraph2JSON
from base import ZugTestBase, PreludeMixin
import es_fixtures

data_dir = os.path.dirname(os.path.realpath(__file__))

sample_props = {'sample_type_id',
                'time_between_clamping_and_freezing',
                'time_between_excision_and_freezing',
                'shortest_dimension', 'oct_embedded', 'submitter_id',
                'intermediate_dimension', 'sample_id',
                'days_to_sample_procurement', 'freezing_method',
                'is_ffpe', 'pathology_report_uuid', 'portions',
                'sample_type', 'days_to_collection', 'initial_weight',
                'current_weight', 'annotations', 'longest_dimension',
                'tumor_code_id', 'tumor_code', 'aliquots'}
project_props = {'name', 'state', 'program', 'primary_site',
                 'project_id', 'disease_type'}
summary_props = {'data_types', 'file_count',
                 'experimental_strategies', 'file_size'}
tss_props = {'project', 'bcr_id', 'code', 'tissue_source_site_id',
             'name'}
portion_props = {'slides', 'portion_id',
                 'creation_datetime', 'is_ffpe',
                 'weight', 'portion_number',
                 'annotations', 'center',
                 'analytes', 'submitter_id'}
analyte_props = {'well_number', 'analyte_type', 'submitter_id',
                 'analyte_id', 'amount', 'aliquots',
                 'a260_a280_ratio', 'concentration',
                 'spectrophotometer_method', 'analyte_type_id',
                 'annotations'}
aliquot_props = {'center', 'submitter_id', 'amount', 'aliquot_id',
                 'concentration', 'source_center', 'annotations'}
annotation_props = {'category', 'status', 'classification',
                    'creator', 'created_datetime', 'notes',
                    'submitter_id', 'annotation_id', 'entity_id',
                    'entity_type', 'case_id'}
file_props = {'data_format', 'related_files', 'center', 'tags',
              'file_name', 'md5sum', 'cases', 'submitter_id',
              'access', 'platform', 'state', 'data_subtype',
              'file_id', 'file_size', 'experimental_strategy',
              'state_comment', 'annotations', 'data_type',
              'uploaded_datetime', 'published_datetime', 'acl',
              'associated_entities', 'archive'}


# TODO move these to gdcdatamodel, they don't belong here

class TestElasticsearchMappings(unittest.TestCase):

    def test_case_project(self):
        props = get_case_es_mapping()['properties']
        self.assertTrue('project' in props)
        print props['project']['properties']
        self.assertTrue('program' in props['project']['properties'])
        self.assertEqual(project_props, set(props['project']['properties']))

    def test_case_summary(self):
        props = get_case_es_mapping()['properties']
        self.assertTrue('summary' in props)
        self.assertEqual(summary_props, set(props['summary']['properties']))

    def test_case_tss(self):
        props = get_case_es_mapping()['properties']
        self.assertTrue('tissue_source_site' in props)
        self.assertEqual(tss_props, set(props['tissue_source_site']['properties']))

    def test_case_samples(self):
        props = get_case_es_mapping()['properties']
        self.assertTrue('samples' in props)
        self.assertEqual(sample_props, set(props['samples']['properties']))

    def test_case_portions(self):
        props = get_case_es_mapping()['properties']
        self.assertTrue('portions' in props['samples']['properties'])
        self.assertEqual(portion_props, set(props['samples']['properties']
                                            ['portions']['properties']))

    def test_case_analytes(self):
        props = get_case_es_mapping()['properties']
        portions = (props['samples']
                    ['properties']['portions']['properties'])
        self.assertTrue('analytes' in portions)
        self.assertEqual(analyte_props, set(portions['analytes']['properties']))

    def test_case_aliquots(self):
        props = get_case_es_mapping()['properties']
        analytes = (props['samples']['properties']
                    ['portions']['properties']
                    ['analytes']['properties'])
        self.assertTrue('aliquots' in analytes)
        self.assertEqual(aliquot_props, set(analytes['aliquots']['properties']))

    def test_case_annotations(self):
        props = get_case_es_mapping()['properties']
        self.assertEqual(annotation_props.union({'entity_submitter_id'}),
                         set(props['annotations']['properties']))

    def test_case_files(self):
        props = get_case_es_mapping()['properties']
        self.assertTrue('files' in props)
        self.assertEqual(file_props.union({'origin'}),
                         set(props['files']['properties']).union(
                             {'annotations', 'associated_entities'}))


class TestPsqlgraph2JSON(PreludeMixin, ZugTestBase):

    def add_file_nodes(self):
        self.live_file = File(
            node_id='file1',
            file_name='TCGA-WR-A838-01A-12R-A406-31_rnaseq_fastq.tar',
            file_size=12916551680,
            md5sum='d7e6cbd40ef2f5b6607cb4af982280a9',
            state='live',
            state_comment=None,
            submitter_id='5cb6bc65-9cd5-45ac-9078-551bc7408906'
        )
        self.to_delete_file = File(
            node_id='file2',
            file_name='a_file_to_be_deleted.txt',
            file_size=5,
            md5sum='foobar',
            state='live',
            state_comment=None,
            submitter_id='5cb6bc65-9cd5-45ac-9078-551bc7408906'
        )
        self.to_delete_file.system_annotations["to_delete"] = True
        self.non_live_file = self.get_fuzzed_node(File, state='uploaded')
        self.data_from_file = self.get_fuzzed_node(File, state='live')
        self.live_file.related_files.append(self.get_fuzzed_node(
            File,
            state="live",
            file_name="a_related_file.bai"
        ))
        with self.g.session_scope():
            aliquot = self.g.nodes(Aliquot).ids(
                '84df0f82-69c4-4cd3-a4bd-f40d2d6ef916').one()
            aliquot.files.append(self.to_delete_file)
            aliquot.files.append(self.live_file)
            aliquot.files.append(self.non_live_file)
            self.live_file.derived_files.append(self.data_from_file)

    def setUp(self):
        super(TestPsqlgraph2JSON, self).setUp()
        es_fixtures.insert(self.g)
        self.add_file_nodes()
        self.convert_documents()

    def convert_documents(self, doc_conv=None):
        doc_conv = doc_conv or PsqlGraph2JSON(self.g)
        with self.g.session_scope():
            doc_conv.cache_database()
        self.case_docs, self.file_docs, self.ann_docs = (
            doc_conv.denormalize_cases())
        if self.case_docs:
            self.case_doc = self.case_docs[0]
        else:
            self.case_doc = None

    def test_case_clinical(self):
        props = self.case_doc
        self.assertTrue('clinical' in props)
        clinical = props['clinical']
        self.assertEqual(clinical['age_at_diagnosis'], 12419)

    def test_filter_non_relevant_annotations(self):
        case = self.get_fuzzed_node(Case)
        annotation = self.get_fuzzed_node(
            Annotation, category='Item flagged DNU')
        with self.g.session_scope() as s:
            f = self.g.nodes(File).ids('file1').first()
            case.projects.append(self.g.nodes(Project).first())
            case.files.append(f)
            case.annotations.append(annotation)
            map(s.merge, (case, annotation))
        self.convert_documents()
        for annotation in self.ann_docs:
            if annotation['entity_type'] == 'case':
                self.assertEqual(
                    annotation['case_id'], annotation['entity_id'])

    def test_case_project(self):
        props = self.case_doc
        self.assertTrue('project' in props)
        actual = set(props['project'].keys())
        self.assertEqual(project_props, actual)

    def test_case_summary(self):
        props = self.case_doc
        self.assertTrue('summary' in props)
        actual = set(props['summary'].keys())
        self.assertEqual(summary_props, actual)

    def test_case_tss(self):
        props = self.case_doc
        self.assertTrue('tissue_source_site' in props)
        actual = set(props['tissue_source_site'].keys())
        self.assertEqual(tss_props, actual)

    def test_case_samples(self):
        props = self.case_doc
        self.assertTrue('samples' in props)
        actual = set(props['samples'][0].keys())
        self.assertEqual(sample_props, actual.union(
            {'annotations', 'aliquots'}))

    def test_case_portions(self):
        props = self.case_doc
        self.assertTrue('portions' in props['samples'][0])
        portion = [p for s in props['samples'] for p in s['portions']
                   if 'slides' not in p][0]
        actual = set(portion.keys())
        self.assertEqual(portion_props, actual.union(
            {'annotations', 'slides', 'center'}))

    def test_case_analytes(self):
        props = self.case_doc
        portions = (props['samples'][0]['portions'][0])
        self.assertTrue('analytes' in portions)
        actual = set(portions['analytes'][0].keys())
        self.assertEqual(analyte_props, actual.union(
            {'annotations'}))

    def test_case_aliquots(self):
        props = self.case_doc
        analytes = (props['samples'][0]['portions'][0]['analytes'][0])
        self.assertTrue('aliquots' in analytes)
        actual = set(analytes['aliquots'][0].keys())
        self.assertEqual(aliquot_props, actual.union(
            {'annotations'}))

    def test_case_files(self):
        props = self.case_doc
        self.assertTrue('files' in props)
        # this makes sure the
        # to_delete/non_live/file-derived_from-file file doesn't show
        # up
        self.assertEqual(len(props["files"]), 1)
        actual = set(props['files'][0].keys())
        self.assertEqual(file_props.union({'origin'}), actual.union(
            {'annotations', 'related_files', 'center', 'data_type',
             'tags', 'data_format', 'platform', 'data_subtype',
             'associated_entities', 'archive', 'experimental_strategy'}))

    def test_omitted_projects(self):
        doc_conv = PsqlGraph2JSON(self.g)
        doc_conv.omitted_projects.add(('TCGA', 'BRCA'))
        self.convert_documents(doc_conv)
        self.assertIsNone(self.case_doc)
