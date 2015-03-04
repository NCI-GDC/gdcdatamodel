import unittest
import logging
from gdcdatamodel.mappings import (
    get_file_es_mapping,
    get_participant_es_mapping,
    get_annotation_es_mapping,
    get_project_es_mapping
)


logging.basicConfig(level=logging.INFO)


class TestElasticsearchMappings(unittest.TestCase):

    def test_file_mapping_import(self):
        get_file_es_mapping()

    def test_participant_mapping_import(self):
        get_participant_es_mapping()

    def test_project_mapping_import(self):
        get_project_es_mapping()

    def test_annotation_mapping_import(self):
        get_annotation_es_mapping()

    def test_file_mapping_top_level(self):
        m = get_file_es_mapping()['properties']
        properties = {'file_name', 'submitter_id', 'related_archives',
                      'file_size', 'state', 'access', 'platform',
                      'participants', 'data_subtype', 'archives',
                      'annotations', 'experimental_strategy', 'data_type',
                      'tags', 'uploaded_datetime', 'file_id', 'related_files',
                      'state_comment', 'published_datetime', 'center',
                      'md5sum', 'data_format', 'acl'}
        self.assertEqual(properties, set(m.keys()))

    def test_participant_mapping_top_level(self):
        m = get_participant_es_mapping()['properties']
        properties = {'files', 'annotations', 'days_to_index', 'submitter_id',
                      'project', 'clinical', 'metadata_files', 'samples',
                      'participant_id', 'summary', 'tissue_source_site'}
        self.assertEqual(properties, set(m.keys()))

    def test_project_mapping_top_level(self):
        m = get_project_es_mapping()['properties']
        properties = {'code', 'name', 'summary', 'state', 'program',
                      'primary_site', 'project_id', 'disease_type'}
        self.assertEqual(properties, set(m.keys()))

    def test_annotation_mapping_top_level(self):
        m = get_annotation_es_mapping()['properties']
        properties = {'category', 'status', 'classification', 'creator',
                      'created_datetime', 'notes', 'submitter_id', 'project',
                      'item_type', 'item_id', 'annotation_id'}
        self.assertEqual(properties, set(m.keys()))
