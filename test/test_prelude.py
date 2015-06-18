from zug.datamodel.prelude import create_prelude_nodes
import base
from gdcdatamodel.models import (
    DataSubtype,
    ProjectMemberOfProgram,
    DataSubtypeMemberOfDataType,
)


class TestPrelude(base.ZugsSimpleTestBase):

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

            for dst in self.driver.nodes(DataSubtype):
                self.assertEqual(len(dst.edges_out), 1)
