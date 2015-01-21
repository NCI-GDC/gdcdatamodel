from zug.datamodel.prelude import create_prelude_nodes
import unittest
from psqlgraph import PsqlGraphDriver


class TestPrelude(unittest.TestCase):

    def setUp(self):
        self.driver = PsqlGraphDriver('localhost', 'test',
                                      'test', 'automated_test')

    def tearDown(self):
        with self.driver.engine.begin() as conn:
            conn.execute('delete from edges')
            conn.execute('delete from nodes')
            conn.execute('delete from voided_edges')
            conn.execute('delete from voided_nodes')
        self.driver.engine.dispose()

    def test_prelude(self):
        create_prelude_nodes(self.driver)
        create_prelude_nodes(self.driver)
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
            property_matches={"name": "ACC"}
        ).with_edge_to_node("member_of", tcga).one()
        clinical = self.driver.node_lookup(
            label="data_type",
            property_matches={"name": "Clinical"}
        ).one()
        self.driver.node_lookup(
            label="data_subtype",
            property_matches={"name": "Diagnostic image"}
        ).with_edge_to_node("member_of", clinical).one()
