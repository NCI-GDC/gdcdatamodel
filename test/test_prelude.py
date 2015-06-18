from zug.datamodel.prelude import create_prelude_nodes
from base import ZugTestBase
from gdcdatamodel.models import (
    DataSubtype,
    ProjectMemberOfProgram,
    DataSubtypeMemberOfDataType,
)


class TestPrelude(ZugTestBase):

    def test_prelude(self):
        self.delete_all_nodes()
        create_prelude_nodes(self.graph)
        create_prelude_nodes(self.graph)
        with self.graph.session_scope():
            self.graph.node_lookup(
                label="center",
                property_matches={"code": "31"}
            ).one()
            self.graph.node_lookup(
                label="tissue_source_site",
                property_matches={"code": "14"}
            ).one()
            self.graph.node_lookup(
                label="tag",
                property_matches={"name": "hg18"}
            ).one()
            tcga = self.graph.node_lookup(
                label="program",
                property_matches={"name": "TCGA"}
            ).one()
            self.graph.node_lookup(
                label="project",
                property_matches={"code": "ACC"}
            ).with_edge_to_node(ProjectMemberOfProgram, tcga).one()
            clinical = self.graph.node_lookup(
                label="data_type",
                property_matches={"name": "Clinical"}
            ).one()
            self.graph.node_lookup(
                label="data_subtype",
                property_matches={"name": "Diagnostic image"}
            ).with_edge_to_node(DataSubtypeMemberOfDataType, clinical).one()

            for dst in self.graph.nodes(DataSubtype):
                self.assertEqual(len(dst.edges_out), 1)
