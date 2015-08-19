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

    def record_error(self, message, key=''):
        self.errors.append({'message': message, 'key': key})

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
        self.entities[0].doc = {'type': 'aliquot', 'centers': {'alias':'test'}}
        self.json_validator.record_errors(self.entities)
        self.assertEqual(self.entities[0].errors[0]['key'], '')
        self.assertEqual(1, len(self.entities[0].errors))

    def test_json_validator_with_wrong_node_type(self):
        self.entities[0].doc = {'type': 'aliquo'}
        self.json_validator.record_errors(self.entities)
        self.assertEqual(self.entities[0].errors[0]['key'], 'type')
        self.assertEqual(1, len(self.entities[0].errors))

    def test_json_validator_with_wrong_property_type(self):
        self.entities[0].doc = {'type': 'aliquot', 'alias': 1, 'centers': {'alias':'test'}}
        self.json_validator.record_errors(self.entities)
        self.assertEqual('alias', self.entities[0].errors[0]['key'])
        self.assertEqual(1, len(self.entities[0].errors))

    def test_json_validator_with_multiple_errors(self):
        self.entities[0].doc = {'type': 'aliquot', 'alias': 1, 'test':'test',
                                'centers': {'alias':'test'}}
        self.json_validator.record_errors(self.entities)
        self.assertEqual(2, len(self.entities[0].errors))

    def test_json_validator_with_nested_error_keys(self):
        self.entities[0].doc = {'type': 'aliquot', 'alias': 'test',
                                'centers': {'alias':True}}
        self.json_validator.record_errors(self.entities)
        self.assertEqual('centers.alias', self.entities[0].errors[0]['key'])


    def test_json_validator_with_multiple_entities(self):
        self.entities[0].doc = {'type': 'aliquot', 'alias': 1, 'test':'test',
                                'centers': {'alias':'test'}}
        entity = MockSubmissionEntity()
        entity.doc = {'type': 'aliquot', 'alias': 'test',
                      'centers': {'alias':'test'}}
        self.entities.append(entity)

        self.json_validator.record_errors(self.entities)
        self.assertEqual(2, len(self.entities[0].errors))
        self.assertEqual(0, len(entity.errors))

    def create_node(self, doc, session):
        cls = Node.get_subclass(doc['type'])
        node = cls(str(uuid.uuid4()))
        node.props = doc['props']
        for key, value in doc['edges']:
            edge = g.nodes.ids(value['id']).first()
            node[key].append(edge)
        session.add(node)
        return node

    def update_link_schema(self, entity, schema):
        self.graph_validator.schemas.schema[entity]['links'] = schema

    def test_graph_validator_without_required_link(self):
        with g.session_scope() as session:
            node = self.create_node({'type':'aliquot',
                                     'props': {'submitter_id': 'test'},
                                     'edges': {}}, session)
            self.entities[0].node=node
            self.update_link_schema(
                'aliquot',
                [{'name': 'analytes',
                 'backref': 'aliquots',
                 'label': 'derived_from',
                 'multiplicity': 'many_to_one',
                 'target_type': 'analyte',
                 'required': True}])
            self.graph_validator.record_errors(g, self.entities)
            self.assertEquals('analytes', self.entities[0].errors[0]['key'])
