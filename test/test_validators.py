import unittest
from gdcdatamodel.validators import GDCJSONValidator, GDCGraphValidator
from psqlgraph import PsqlGraphDriver
from gdcdatamodel.models import *
from gdcapi.resources.submission.transaction import SubmissionEntity
import logging

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'
g = PsqlGraphDriver(host, user, password, database)

class TestValidators(unittest.TestCase):
    def setUp(self):
        pass

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
        entity.parse({'type':'aliquot'})
        pass

