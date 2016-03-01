from gdcdatamodel import models as md
from psqlgraph import Node, Edge, PsqlGraphDriver

import unittest

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'
g = PsqlGraphDriver(host, user, password, database)


class TestValidators(unittest.TestCase):

    @staticmethod
    def new_portion():
        portion = md.Portion(**{
            'node_id': 'case1',
            'is_ffpe': False,
            'portion_number': u'01',
            'project_id': u'CGCI-BLGSP',
            'state': 'validated',
            'submitter_id': u'PORTION-1',
            'weight': 54.0
        })
        portion.acl = ['acl1']
        portion.sysan.update({'key1': 'val1'})
        return portion

    @staticmethod
    def new_analyte():
        return md.Analyte(**{
            'node_id': 'analyte1',
            'analyte_type': u'Repli-G (Qiagen) DNA',
            'analyte_type_id': u'W',
            'project_id': u'CGCI-BLGSP',
            'state': 'validated',
            'submitter_id': u'TCGA-AR-A1AR-01A-31W',
        })

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
        conn.execute('delete from versioned_nodes')
        conn.execute('delete from _voided_nodes')
        conn.execute('delete from _voided_edges')
        conn.close()

    def test_round_trip(self):
        with g.session_scope() as session:
            portion = self.new_portion()
            analyte = self.new_analyte()
            portion.analytes = [analyte]
            session.add(portion)

        with g.session_scope() as session:
            portion = g.nodes(md.Portion).one()
            v_node = md.VersionedNode.clone(portion)
            session.add(v_node)

        with g.session_scope():
            v_node = g.nodes(md.VersionedNode).one()

        self.assertEqual(v_node.properties['is_ffpe'], False)
        self.assertEqual(v_node.properties['state'], 'validated')
        self.assertEqual(v_node.properties['state'], 'validated')
        self.assertEqual(v_node.system_annotations, {'key1': 'val1'})
        self.assertEqual(v_node.acl, ['acl1'])
        self.assertEqual(v_node.neighbors, ['analyte1'])
        self.assertIsNotNone(v_node.versioned)
        self.assertIsNotNone(v_node.key)

    def test_versions_property(self):
        with g.session_scope() as session:
            portion = self.new_portion()
            analyte = self.new_analyte()
            portion.analytes = [analyte]
            session.add(portion)

        with g.session_scope() as session:
            portion = g.nodes(md.Portion).one()
            v_node = md.VersionedNode.clone(portion)
            session.add(v_node)

        with g.session_scope():
            portion = g.nodes(md.Portion).one()
            portion._versions.one()

        with self.assertRaises(RuntimeError):
            portion._versions.one()

        with g.session_scope() as s:
            portion.get_versions(s).one()
