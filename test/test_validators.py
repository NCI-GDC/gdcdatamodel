import uuid
from copy import copy

from gdcdatamodel.validators import GDCJSONValidator, GDCGraphValidator
from gdcdatamodel.models import *

from test.conftest import BaseTestCase


class MockSubmissionEntity(object):
    def __init__(self):
        self.errors = []
        self.node = None
        self.doc = {}

    def record_error(self, message, **kwargs):
        self.errors.append(dict(message=message, **kwargs))


class TestValidators(BaseTestCase):
    def setUp(self):
        super(TestValidators, self).setUp()

        self.graph_validator = GDCGraphValidator()
        self.json_validator = GDCJSONValidator()
        self.entities = [MockSubmissionEntity()]

    def test_json_validator_with_insufficient_properties(self):
        self.entities[0].doc = {'type': 'aliquot',
                                'centers': {'submitter_id': 'test'}}
        self.json_validator.record_errors(self.entities)
        self.assertEqual(self.entities[0].errors[0]['keys'], ['submitter_id'])
        self.assertEqual(1, len(self.entities[0].errors))

    def test_json_validator_with_wrong_node_type(self):
        self.entities[0].doc = {'type': 'aliquo'}
        self.json_validator.record_errors(self.entities)
        self.assertEqual(self.entities[0].errors[0]['keys'], ['type'])
        self.assertEqual(1, len(self.entities[0].errors))

    def test_json_validator_with_wrong_property_type(self):
        self.entities[0].doc = {'type': 'aliquot',
                                'submitter_id': 1, 'centers': {'submitter_id': 'test'}}
        self.json_validator.record_errors(self.entities)
        self.assertEqual(['submitter_id'], self.entities[0].errors[0]['keys'])
        self.assertEqual(1, len(self.entities[0].errors))

    def test_json_validator_with_multiple_errors(self):
        self.entities[0].doc = {'type': 'aliquot', 'submitter_id': 1,
                                'test': 'test',
                                'centers': {'submitter_id': 'test'}}
        self.json_validator.record_errors(self.entities)
        self.assertEqual(2, len(self.entities[0].errors))

    def test_json_validator_with_nested_error_keys(self):
        self.entities[0].doc = {'type': 'aliquot', 'submitter_id': 'test',
                                'centers': {'submitter_id': True}}
        self.json_validator.record_errors(self.entities)
        self.assertEqual(['centers'], self.entities[0].errors[0]['keys'])

    def test_json_validator_with_multiple_entities(self):
        self.entities[0].doc = {'type': 'aliquot', 'submitter_id': 1, 'test': 'test',
                                'centers': {'submitter_id': 'test'}}
        entity = MockSubmissionEntity()
        entity.doc = {'type': 'aliquot', 'submitter_id': 'test',
                      'centers': {'submitter_id': 'test'}}
        self.entities.append(entity)

        self.json_validator.record_errors(self.entities)
        self.assertEqual(2, len(self.entities[0].errors))
        self.assertEqual(0, len(entity.errors))

    def test_json_validator_with_array_prop(self):
        entity_doc = {
            'type': 'diagnosis',
            'submitter_id': 'test',
            'age_at_diagnosis': 10,
            'primary_diagnosis': 'Abdominal desmoid',
            "primary_disease": True,  # now required property with dictionary 2.4.0
            "morphology": "8000/0",
            "tissue_or_organ_of_origin": "Abdomen, NOS",
            "site_of_resection_or_biopsy": "Abdomen, NOS",
        }

        def mock_doc(sites_of_involvement):
            mock = MockSubmissionEntity()
            mock.doc = copy(entity_doc)
            mock.doc["sites_of_involvement"] = sites_of_involvement
            return mock

        # invalid array value at index 0
        self.entities[0] = mock_doc(["Right"])

        # invalid array value at index 1
        self.entities.append(mock_doc(["Cervix", "Right"]))

        # valid array values
        self.entities.append(mock_doc(["Cervix", "Ovary, NOS"]))

        self.json_validator.record_errors(self.entities)
        self.assertEqual(1, len(self.entities[0].errors))
        self.assertEqual(1, len(self.entities[1].errors))
        self.assertEqual(0, len(self.entities[2].errors))
        self.assertEqual(["sites_of_involvement.0"], self.entities[0].errors[0]["keys"])
        self.assertEqual(["sites_of_involvement.1"], self.entities[1].errors[0]["keys"])

    def create_node(self, doc, session):
        cls = Node.get_subclass(doc['type'])
        node = cls(str(uuid.uuid4()))
        node.props = doc['props']
        for key, value in doc['edges'].items():
            for target_id in value:
                edge = self.g.nodes().ids(target_id).first()
                node[key].append(edge)
        session.add(node)
        return node

    def update_schema(self, entity, key, schema):
        self.graph_validator.schemas.schema[entity][key] = schema

    def test_graph_validator_without_required_link(self):
        with self.g.session_scope() as session:
            node = self.create_node({'type': 'aliquot',
                                     'props': {'submitter_id': 'test'},
                                     'edges': {}}, session)
            self.entities[0].node = node
            self.update_schema(
                'aliquot',
                'links',
                [{'name': 'analytes',
                  'backref': 'aliquots',
                  'label': 'derived_from',
                  'multiplicity': 'many_to_one',
                  'target_type': 'analyte',
                  'required': True}])
            self.graph_validator.record_errors(self.g, self.entities)
            self.assertEqual(['analytes'], self.entities[0].errors[0]['keys'])

    def test_graph_validator_with_exclusive_link(self):
        with self.g.session_scope() as session:
            analyte = self.create_node(
                {'type': 'analyte',
                 'props': {'submitter_id': 'test',
                           'analyte_type_id': 'D',
                           'analyte_type': 'DNA'},
                 'edges': {}}, session)
            sample = self.create_node({'type': 'sample',
                                       'props': {'submitter_id': 'test',
                                                 'sample_type': 'DNA',
                                                 'sample_type_id': '01'},
                                       'edges': {}}, session)

            node = self.create_node(
                {'type': 'aliquot',
                 'props': {'submitter_id': 'test'},
                 'edges': {'analytes': [analyte.node_id],
                           'samples': [sample.node_id]}}, session)
            self.entities[0].node = node
            self.update_schema(
                'aliquot',
                'links',
                [{'exclusive': True,
                  'required': True,
                  'subgroup': [
                      {'name': 'analytes',
                       'backref': 'aliquots',
                       'label': 'derived_from',
                       'multiplicity': 'many_to_one',
                       'target_type': 'analyte'},
                      {'name': 'samples',
                       'backref': 'aliquots',
                       'label': 'derived_from',
                       'multiplicity': 'many_to_one',
                       'target_type': 'sample'}]}])
            self.graph_validator.record_errors(self.g, self.entities)
            self.assertEqual(['analytes', 'samples'],
                              self.entities[0].errors[0]['keys'])

    def test_graph_validator_with_wrong_multiplicity(self):
        with self.g.session_scope() as session:
            analyte = self.create_node({'type': 'analyte',
                                        'props': {'submitter_id': 'test',
                                                  'analyte_type_id': 'D',
                                                  'analyte_type': 'DNA'},
                                        'edges': {}}, session)

            analyte_b = self.create_node({'type': 'analyte',
                                          'props': {'submitter_id': 'testb',
                                                    'analyte_type_id': 'H',
                                                    'analyte_type': 'RNA'},
                                          'edges': {}}, session)

            node = self.create_node({'type': 'aliquot',
                                     'props': {'submitter_id': 'test'},
                                     'edges': {'analytes': [analyte.node_id,
                                                            analyte_b.node_id]}},
                                    session)
            self.entities[0].node = node
            self.update_schema(
                'aliquot',
                'links',
                [{'exclusive': False,
                  'required': True,
                  'subgroup': [
                      {'name': 'analytes',
                       'backref': 'aliquots',
                       'label': 'derived_from',
                       'multiplicity': 'many_to_one',
                       'target_type': 'analyte'},
                      {'name': 'samples',
                       'backref': 'aliquots',
                       'label': 'derived_from',
                       'multiplicity': 'many_to_one',
                       'target_type': 'sample'}]}])
            self.graph_validator.record_errors(self.g, self.entities)
            self.assertEqual(['analytes'], self.entities[0].errors[0]['keys'])

    def test_graph_validator_with_correct_node(self):
        with self.g.session_scope() as session:
            analyte = self.create_node({'type': 'analyte',
                                        'props': {'submitter_id': 'test',
                                                  'analyte_type_id': 'D',
                                                  'analyte_type': 'DNA'},
                                        'edges': {}}, session)

            node = self.create_node({'type': 'aliquot',
                                     'props': {'submitter_id': 'test'},
                                     'edges': {'analytes': [analyte.node_id]}},
                                    session)
            self.entities[0].node = node
            self.update_schema(
                'aliquot',
                'links',
                [{'exclusive': False,
                  'required': True,
                  'subgroup': [
                      {'name': 'analytes',
                       'backref': 'aliquots',
                       'label': 'derived_from',
                       'multiplicity': 'many_to_one',
                       'target_type': 'analyte'},
                      {'name': 'samples',
                       'backref': 'aliquots',
                       'label': 'derived_from',
                       'multiplicity': 'many_to_one',
                       'target_type': 'sample'}]}])
            self.graph_validator.record_errors(self.g, self.entities)
            self.assertEqual(0, len(self.entities[0].errors))

    def test_graph_validator_with_existing_unique_keys(self):
        with self.g.session_scope() as session:
            node = self.create_node({'type': 'data_format',
                                     'props': {'name': 'test'},
                                     'edges': {}},
                                    session)
            node = self.create_node({'type': 'data_format',
                                     'props': {'name': 'test'},
                                     'edges': {}},
                                    session)
            self.update_schema('data_format', 'uniqueKeys', [['name']])
            self.entities[0].node = node
            self.graph_validator.record_errors(self.g, self.entities)
            self.assertEqual(['name'], self.entities[0].errors[0]['keys'])

    def test_graph_validator_with_existing_unique_keys_for_different_node_types(self):
        with self.g.session_scope() as session:
            node = self.create_node({'type': 'sample',
                                     'props': {'submitter_id': 'test','project_id':'A'},
                                     'edges': {}},
                                    session)
            node = self.create_node({'type': 'aliquot',
                                     'props': {'submitter_id': 'test', 'project_id':'A'},
                                     'edges': {}},
                                    session)
            self.update_schema('data_format', 'uniqueKeys', [['submitter_id', 'project_id']])
            self.entities[0].node = node
            self.graph_validator.record_errors(self.g, self.entities)
            # Check (project_id, submitter_id) uniqueness is captured
            self.assertTrue(any({'project_id', 'submitter_id'} == set(e['keys'])
                                for e in self.entities[0].errors))
            # Check that missing edges is captured
            self.assertTrue(any({'analytes', 'samples'} == set(e['keys'])
                                for e in self.entities[0].errors))
