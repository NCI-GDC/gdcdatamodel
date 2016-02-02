import unittest
import logging
from gdcdatamodel.mappings import (
    get_file_es_mapping,
    get_case_es_mapping,
    get_annotation_es_mapping,
    get_project_es_mapping,
    TOP_LEVEL_IDS,
)


logging.basicConfig(level=logging.INFO)


class TestElasticsearchMappings(unittest.TestCase):

    def test_file_mapping_import(self):
        get_file_es_mapping()

    def test_case_mapping_import(self):
        get_case_es_mapping()

    def test_project_mapping_import(self):
        get_project_es_mapping()

    def test_annotation_mapping_import(self):
        get_annotation_es_mapping()

    def test_file_mapping_top_level(self):
        m = get_file_es_mapping()['properties']
        properties = {'file_name', 'submitter_id', 'file_size',
                      'state', 'access', 'platform', 'cases',
                      'data_subtype', 'archive', 'annotations',
                      'experimental_strategy', 'data_type', 'tags',
                      'uploaded_datetime', 'file_id', 'related_files',
                      'state_comment', 'published_datetime', 'center',
                      'md5sum', 'data_format', 'acl', 'origin',
                      'associated_entities', 'project_id', 'error_type',
                      'created_datetime', 'updated_datetime', 'file_state'}
        self.assertEqual(properties, set(m.keys()))

    def test_case_mapping_top_level(self):
        m = get_case_es_mapping()['properties']
        properties = {'files', 'annotations', 'days_to_index',
                      'submitter_id', 'project', 'clinical',
                      'metadata_files', 'samples', 'case_id',
                      'summary', 'tissue_source_site', 'aliquots',
                      'sample_ids', 'portion_ids',
                      'submitter_portion_ids',
                      'submitter_aliquot_ids',
                      'submitter_analyte_ids', 'analyte_ids',
                      'aliquot_ids', 'submitter_sample_ids',
                      'slide_ids', 'submitter_slide_ids',
                      'project_id', 'created_datetime',
                      'updated_datetime', 'state'}
        self.assertEqual(properties, set(m.keys()))

    def test_project_mapping_top_level(self):
        m = get_project_es_mapping()['properties']
        properties = {'name', 'summary', 'state', 'program',
                      'primary_site', 'project_id', 'disease_type',
                      'dbgap_accession_number'}
        self.assertEqual(properties, set(m.keys()))

    def test_annotation_mapping_top_level(self):
        m = get_annotation_es_mapping()['properties']
        properties = {'category', 'status', 'classification',
                      'creator', 'created_datetime', 'notes',
                      'submitter_id', 'project', 'entity_type',
                      'entity_id', 'annotation_id', 'case_id',
                      'entity_submitter_id', 'case_submitter_id',
                      'project_id', 'created_datetime',
                      'updated_datetime'}
        self.assertEqual(properties, set(m.keys()))
