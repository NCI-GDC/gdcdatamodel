import unittest
import uuid
from gdcdatamodel.validators import GDCJSONValidator, GDCGraphValidator
from psqlgraph import PsqlGraphDriver
from gdcdatamodel.models import *

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'
g = PsqlGraphDriver(host, user, password, database)


class MockSubmissionEntity(object):
    def __init__(self):
        self.errors = []
        self.node = None
        self.doc = {}

    def record_error(self, message, **kwargs):
        self.errors.append(dict(message=message, **kwargs))


class TestValidators(unittest.TestCase):
    def setUp(self):
        self.graph_validator = GDCGraphValidator()
        self.json_validator = GDCJSONValidator()
        self.entities = [MockSubmissionEntity()]

    def tearDown(self):
        self._clear_tables()

    def _clear_tables(self):
        conn = g.engine.connect()
        conn.execute('commit')
        for table in Node().get_subclass_table_names():
            if table != Node.__tablename__:
                conn.execute('delete from {}'.format(table))
        for table in Edge.get_subclass_table_names():
            if table != Edge.__tablename__:
                conn.execute('delete from {}'.format(table))
        conn.execute('delete from _voided_nodes')
        conn.execute('delete from _voided_edges')
        conn.close()

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

    def create_node(self, doc, session):
        cls = Node.get_subclass(doc['type'])
        node = cls(str(uuid.uuid4()))
        node.props = doc['props']
        for key, value in doc['edges'].iteritems():
            for target_id in value:
                edge = g.nodes().ids(target_id).first()
                node[key].append(edge)
        session.add(node)
        return node

    def update_schema(self, entity, key, schema):
        self.graph_validator.schemas.schema[entity][key] = schema

    def test_graph_validator_without_required_link(self):
        with g.session_scope() as session:
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
            self.graph_validator.record_errors(g, self.entities)
            self.assertEquals(['analytes'], self.entities[0].errors[0]['keys'])

    def test_graph_validator_with_exclusive_link(self):
        with g.session_scope() as session:
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
            self.graph_validator.record_errors(g, self.entities)
            self.assertEquals(['analytes', 'samples'],
                              self.entities[0].errors[0]['keys'])

    def test_graph_validator_with_wrong_multiplicity(self):
        with g.session_scope() as session:
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
            self.graph_validator.record_errors(g, self.entities)
            self.assertEquals(['analytes'], self.entities[0].errors[0]['keys'])

    def test_graph_validator_with_correct_node(self):
        with g.session_scope() as session:
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
            self.graph_validator.record_errors(g, self.entities)
            self.assertEquals(0, len(self.entities[0].errors))

    def test_graph_validator_with_existing_unique_keys(self):
        with g.session_scope() as session:
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
            self.graph_validator.record_errors(g, self.entities)
            self.assertEquals(['name'], self.entities[0].errors[0]['keys'])
