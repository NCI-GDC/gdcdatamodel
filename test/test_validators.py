import unittest
from gdcdatamodel.validators import GDCJSONValidator, GDCGraphValidator
from psqlgraph import PsqlGraphDriver
from gdcdatamodel.models import *
from gdcapi.resources.submission.transaction import SubmissionEntity

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'
g = PsqlGraphDriver(host, user, password, database)


class TestValidators(unittest.TestCase):
    def setUp(self):
        self.graph_validator = GDCGraphValidator()
        self.json_validator = GDCJSONValidator()

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
        entity = SubmissionEntity()
        entity.parse({'type': 'aliquot'})
        self.json_validator.record_errors([entity])
        self.assertEqual(entity.errors[0]['key'], '')
        self.assertEqual(1, len(entity.errors))

    def test_json_validator_with_wrong_node_type(self):
        entity = SubmissionEntity()
        entity.parse({'type': 'aliquo'})
        self.json_validator.record_errors([entity])
        self.assertEqual(entity.errors[0]['key'], 'type')
        self.assertEqual(1, len(entity.errors))

    def test_json_validator_with_wrong_property_type(self):
        entity = SubmissionEntity()
        entity.parse({'type': 'aliquot', 'alias': 1})
        self.json_validator.record_errors([entity])
        self.assertEqual('alias', entity.errors[0]['key'])
        self.assertEqual(1, len(entity.errors))
