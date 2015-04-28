from zug.datamodel.prelude import create_prelude_nodes
import unittest
from psqlgraph import PsqlGraphDriver, Node, Edge
from gdcdatamodel.models import (
    ProjectMemberOfProgram,
    DataSubtypeMemberOfDataType,
)


class TestPrelude(unittest.TestCase):

    def setUp(self):
        self.driver = PsqlGraphDriver(
            'localhost', 'test', 'test', 'automated_test')
        self.clear_tables()

    def tearDown(self):
        self.clear_tables()

    def clear_tables(self):
        with self.driver.engine.begin() as conn:
            for table in Node().get_subclass_table_names():
                if table != Node.__tablename__:
                    conn.execute('delete from {}'.format(table))
            for table in Edge().get_subclass_table_names():
                if table != Edge.__tablename__:
                    conn.execute('delete from {}'.format(table))
            conn.execute('delete from _voided_nodes')
            conn.execute('delete from _voided_edges')
        self.driver.engine.dispose()

    def test_prelude(self):
        create_prelude_nodes(self.driver)
        create_prelude_nodes(self.driver)
        with self.driver.session_scope():
            self.driver.node_lookup(
                label="center",
                property_matches={"code": "31"}
            ).one()
            self.driver.node_lookup(
                label="tissue_source_site",
                property_matches={"code": "14"}
            ).one()
            self.driver.node_lookup(
                label="tag",
                property_matches={"name": "hg18"}
            ).one()
            tcga = self.driver.node_lookup(
                label="program",
                property_matches={"name": "TCGA"}
            ).one()
            self.driver.node_lookup(
                label="project",
                property_matches={"code": "ACC"}
            ).with_edge_to_node(ProjectMemberOfProgram, tcga).one()
            clinical = self.driver.node_lookup(
                label="data_type",
                property_matches={"name": "Clinical"}
            ).one()
            self.driver.node_lookup(
                label="data_subtype",
                property_matches={"name": "Diagnostic image"}
            ).with_edge_to_node(DataSubtypeMemberOfDataType, clinical).one()

            for dst in self.driver.nodes().labels('data_subtype'):
                self.assertEqual(len(dst.edges_out), 1)
