import logging
import unittest
import os
from zug.datamodel.prelude import create_prelude_nodes
from zug.datamodel import xml2psqlgraph, bcr_xml_mapping
from zug.datamodel.psqlgraph2json import PsqlGraph2JSON
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from gdcdatamodel import (
    node_avsc_object, edge_avsc_object,
    get_participant_es_mapping, get_file_es_mapping
)
from psqlgraph import PsqlGraphDriver, Edge

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

data_dir = os.path.dirname(os.path.realpath(__file__))

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'

converter = xml2psqlgraph.xml2psqlgraph(
    xml_mapping=bcr_xml_mapping,
    host=host,
    user=user,
    password=password,
    database=database,
    node_validator=AvroNodeValidator(node_avsc_object),
    edge_validator=AvroEdgeValidator(edge_avsc_object),
)
g = PsqlGraphDriver(
    host=host,
    user=user,
    password=password,
    database=database,
    node_validator=AvroNodeValidator(node_avsc_object),
    edge_validator=AvroEdgeValidator(edge_avsc_object),
)

sample_props = {'sample_type_id',
                'time_between_clamping_and_freezing',
                'time_between_excision_and_freezing',
                'shortest_dimension', 'oct_embedded', 'submitter_id',
                'intermediate_dimension', 'sample_id',
                'days_to_sample_procurement', 'freezing_method',
                'is_ffpe', 'pathology_report_uuid', 'portions',
                'sample_type', 'days_to_collection', 'initial_weight',
                'current_weight', 'annotations', 'longest_dimension'}
project_props = {'code', 'name', 'state', 'program', 'primary_site',
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
                    'submitter_id', 'annotation_id', 'item_id',
                    'item_type', 'project'}
file_props = {'data_format', 'related_files', 'center', 'tags',
              'file_name', 'md5sum', 'participants',
              'submitter_id', 'access', 'platform', 'state',
              'data_subtype', 'file_id', 'file_size',
              'experimental_strategy', 'state_comment',
              'annotations', 'archives', 'related_archives', 'data_type',
              'uploaded_datetime', 'published_datetime', 'acl'}


class TestElasticsearchMappings(unittest.TestCase):

    def test_participant_project(self):
        props = get_participant_es_mapping()['properties']
        self.assertTrue('project' in props)
        print props['project']['properties']
        self.assertTrue('program' in props['project']['properties'])
        self.assertEqual(project_props.symmetric_difference(
            set(props['project']['properties'])), set([]))

    def test_participant_summary(self):
        props = get_participant_es_mapping()['properties']
        self.assertTrue('summary' in props)
        self.assertEqual(summary_props.symmetric_difference(
            set(props['summary']['properties'])), set([]))

    def test_participant_tss(self):
        props = get_participant_es_mapping()['properties']
        self.assertTrue('tissue_source_site' in props)
        self.assertEqual(tss_props.symmetric_difference(
            set(props['tissue_source_site']['properties'])), set([]))

    def test_participant_samples(self):
        props = get_participant_es_mapping()['properties']
        self.assertTrue('samples' in props)
        self.assertEqual(sample_props.symmetric_difference(
            set(props['samples']['properties'])), set([]))

    def test_participant_portions(self):
        props = get_participant_es_mapping()['properties']
        self.assertTrue('portions' in props['samples']['properties'])
        self.assertEqual(portion_props.symmetric_difference(
            set(props['samples']['properties']
                ['portions']['properties'])), set([]))

    def test_participant_analytes(self):
        props = get_participant_es_mapping()['properties']
        portions = (props['samples']
                    ['properties']['portions']['properties'])
        self.assertTrue('analytes' in portions)
        self.assertEqual(analyte_props.symmetric_difference(
            set(portions['analytes']['properties'])), set([]))

    def test_participant_aliquots(self):
        props = get_participant_es_mapping()['properties']
        analytes = (props['samples']['properties']
                    ['portions']['properties']
                    ['analytes']['properties'])
        self.assertTrue('aliquots' in analytes)
        self.assertEqual(aliquot_props.symmetric_difference(
            set(analytes['aliquots']['properties'])), set([]))

    def test_participant_annotations(self):
        props = get_participant_es_mapping()['properties']
        self.assertTrue('annotations' in props)
        self.assertEqual(annotation_props.symmetric_difference(
            set(props['annotations']['properties'])), set(['project']))

    def test_participant_files(self):
        props = get_participant_es_mapping()['properties']
        self.assertTrue('files' in props)
        self.assertEqual(file_props.symmetric_difference(
            set(props['files']['properties'])), set([]))


class TestPsqlgraph2JSON(unittest.TestCase):

    def add_req_nodes(self):
        for code in ['01', '02', '05', '07', '09', '13']:
            g.node_merge(node_id=code, label='center', properties={
                'center_type': 'center_type',
                'code': code,
                'name': 'name',
                'namespace': 'namespace',
                'short_name': 'short_name'})
        g.node_merge(node_id='tss', label='tissue_source_site',
                     properties={'name': 'BRCA', 'bcr_id': 'id', 'code': 'AR',
                                 'name': u"name", 'project': 'Mesothelioma'})
        g.node_merge(node_id='project', label='project', properties={
            'code': 'BRCA', 'primary_site': 'primary site',
            'name': 'project name', 'state': 'legacy',
            'disease_type': 'disease type'})

    def add_file_nodes(self):
        g.node_merge(node_id='file1', label='file', properties={
            u'file_name': u'TCGA-WR-A838-01A-12R-A406-31_rnaseq_fastq.tar',
            u'file_size': 12916551680,
            u'md5sum': u'd7e6cbd40ef2f5b6607cb4af982280a9',
            u'state': u'submitted',
            u'state_comment': None,
            u'submitter_id': u'5cb6bc65-9cd5-45ac-9078-551bc7408906'})

        with g.session_scope():
            ids = {'src_id': 'file1',
                   'dst_id': '84df0f82-69c4-4cd3-a4bd-f40d2d6ef916'}
            if not g.edge_lookup(**ids).count():
                g.edge_insert(Edge(label='data_from', **ids))

    def setUp(self):
        self.add_req_nodes()
        with open(os.path.join(data_dir, 'sample_biospecimen.xml')) as f:
            xml = f.read()
        converter.xml2psqlgraph(xml)
        converter.export()
        self.add_file_nodes()
        doc_conv = PsqlGraph2JSON(g)
        with g.session_scope():
            doc_conv.cache_database()
        self.part_docs, self.file_docs, self.ann_docs = (
            doc_conv.denormalize_participants())
        self.part_doc = self.part_docs[0]

    def tearDown(self):
        with g.engine.begin() as conn:
            conn.execute('delete from edges')
            conn.execute('delete from nodes')
            conn.execute('delete from voided_edges')
            conn.execute('delete from voided_nodes')
        g.engine.dispose()

    def test_participant_project(self):
        props = self.part_doc
        self.assertTrue('project' in props)
        self.assertEqual(project_props.symmetric_difference(
            set(props['project'].keys())), set(['program']))

    def test_participant_summary(self):
        props = self.part_doc
        self.assertTrue('summary' in props)
        self.assertEqual(summary_props.symmetric_difference(
            set(props['summary'].keys())), set([]))

    def test_participant_tss(self):
        props = self.part_doc
        self.assertTrue('tissue_source_site' in props)
        self.assertEqual(tss_props.symmetric_difference(
            set(props['tissue_source_site'].keys())), set([]))

    def test_participant_samples(self):
        props = self.part_doc
        self.assertTrue('samples' in props)
        self.assertEqual(sample_props.symmetric_difference(
            set(props['samples'][0].keys())), set(['annotations']))

    def test_participant_portions(self):
        props = self.part_doc
        self.assertTrue('portions' in props['samples'][0])
        portion = [p for s in props['samples'] for p in s['portions']
                   if 'slides' not in p][0]
        self.assertEqual(portion_props.symmetric_difference(
            set(portion.keys())), set(['center', 'annotations', 'slides']))

    def test_participant_analytes(self):
        props = self.part_doc
        portions = (props['samples'][0]['portions'][0])
        self.assertTrue('analytes' in portions)
        self.assertEqual(analyte_props.symmetric_difference(
            set(portions['analytes'][0].keys())), set(['annotations']))

    def test_participant_aliquots(self):
        props = self.part_doc
        analytes = (props['samples'][0]['portions'][0]['analytes'][0])
        self.assertTrue('aliquots' in analytes)
        self.assertEqual(aliquot_props.symmetric_difference(
            set(analytes['aliquots'][0].keys())), set(['annotations']))

    def test_participant_files(self):
        props = self.part_doc
        self.assertTrue('files' in props)
        self.assertEqual(file_props.symmetric_difference(
            set(props['files'][0].keys())
        ), set(['related_files', 'center', 'tags', 'data_format', 'platform',
                'archives', 'annotations', 'experimental_strategy',
                'data_subtype', 'related_archives', 'data_type']))
