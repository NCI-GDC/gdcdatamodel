from datetime import datetime
import logging
import unittest

from psqlgraph import Edge, Node, PsqlGraphDriver
from psqlgraph.exc import ValidationError

from gdcdatamodel import models as md

logging.basicConfig(level=logging.INFO)


class TestDataModel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        host = 'localhost'
        user = 'test'
        password = 'test'
        database = 'automated_test'
        cls.g = PsqlGraphDriver(host, user, password, database)

        cls._clear_tables()

    def tearDown(self):
        self._clear_tables()

    @classmethod
    def _clear_tables(cls):
        conn = cls.g.engine.connect()
        conn.execute('commit')
        for table in Node().get_subclass_table_names():
            if table != Node.__tablename__:
                conn.execute('delete from {}'.format(table))
        for table in Edge.get_subclass_table_names():
            if table != Edge.__tablename__:
                conn.execute('delete from {}'.format(table))
        conn.execute('delete from versioned_nodes')
        conn.execute('delete from _voided_nodes')
        conn.execute('delete from _voided_edges')
        conn.close()

    def test_type_validation(self):
        f = md.File()
        with self.assertRaises(ValidationError):
            f.file_size = '0'
        f.file_size = 0

        f = md.File()
        with self.assertRaises(ValidationError):
            f.file_name = 0
        f.file_name = '0'

        s = md.Sample()
        with self.assertRaises(ValidationError):
            s.is_ffpe = 'false'
        s.is_ffpe = False

        s = md.Slide()
        with self.assertRaises(ValidationError):
            s.percent_necrosis = '0.0'
        s.percent_necrosis = 0.0

    def test_link_clobber_prevention(self):
        with self.assertRaises(AssertionError):
            md.EdgeFactory(
                'Testedge',
                'test',
                'sample',
                'aliquot',
                'aliquots',
                '_uncontended_backref',
            )

    def test_backref_clobber_prevention(self):
        with self.assertRaises(AssertionError):
            md.EdgeFactory(
                'Testedge',
                'test',
                'sample',
                'aliquot',
                '_uncontended_link',
                'samples',
            )

    def test_created_datetime_hook(self):
        """Test setting created/updated datetime when a node is created."""
        time_before = datetime.utcnow().isoformat()

        with self.g.session_scope() as s:
            s.merge(md.Case('case1'))

        time_after = datetime.utcnow().isoformat()

        with self.g.session_scope():
            case = self.g.nodes(md.Case).one()

            # Compare against the time both before and after the write to
            # ensure the comparison is fair.
            assert time_before < case.created_datetime < time_after
            assert time_before < case.updated_datetime < time_after

    def test_updated_datetime_hook(self):
        """Test setting updated datetime when a node is updated."""
        with self.g.session_scope() as s:
            s.merge(md.Case('case1'))

        with self.g.session_scope():
            case = self.g.nodes(md.Case).one()
            old_created_datetime = case.created_datetime
            old_updated_datetime = case.updated_datetime

            case.primary_site = 'Kidney'

        with self.g.session_scope():
            updated_case = self.g.nodes(md.Case).one()
            assert updated_case.created_datetime == old_created_datetime
            assert updated_case.updated_datetime > old_updated_datetime

    def test_no_datetime_update_for_new_edge(self):
        """Verify new inbound edges do not affect a node's updated datetime."""
        with self.g.session_scope() as s:
            s.merge(md.Case('case1'))

        with self.g.session_scope() as s:
            case = self.g.nodes(md.Case).one()
            old_created_datetime = case.created_datetime
            old_updated_datetime = case.updated_datetime

            sample = s.merge(md.Sample('sample1'))
            case.samples.append(sample)

        with self.g.session_scope():
            updated_case = self.g.nodes(md.Case).one()
            assert updated_case.created_datetime == old_created_datetime
            assert updated_case.updated_datetime == old_updated_datetime
