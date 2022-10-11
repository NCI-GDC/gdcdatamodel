from test.conftest import BaseTestCase

from gdcdatamodel import models as md


class TestValidators(BaseTestCase):

    @staticmethod
    def new_portion():
        portion = md.Portion(**{
            'node_id': 'case1',
            'is_ffpe': False,
            'portion_number': '01',
            'project_id': 'CGCI-BLGSP',
            'state': 'validated',
            'submitter_id': 'PORTION-1',
            'weight': 54.0
        })
        portion.acl = ['acl1']
        portion.sysan.update({'key1': 'val1'})
        return portion

    @staticmethod
    def new_analyte():
        return md.Analyte(**{
            'node_id': 'analyte1',
            'analyte_type': 'Repli-G (Qiagen) DNA',
            'analyte_type_id': 'W',
            'project_id': 'CGCI-BLGSP',
            'state': 'validated',
            'submitter_id': 'TCGA-AR-A1AR-01A-31W',
        })

    def test_round_trip(self):
        with self.g.session_scope() as session:
            portion = self.new_portion()
            analyte = self.new_analyte()
            portion.analytes = [analyte]
            session.add(portion)

        with self.g.session_scope() as session:
            portion = self.g.nodes(md.Portion).one()
            v_node = md.VersionedNode.clone(portion)
            session.add(v_node)

        with self.g.session_scope():
            v_node = self.g.nodes(md.VersionedNode).one()

        self.assertEqual(v_node.properties['is_ffpe'], False)
        self.assertEqual(v_node.properties['state'], 'validated')
        self.assertEqual(v_node.properties['state'], 'validated')
        self.assertEqual(v_node.system_annotations, {'key1': 'val1'})
        self.assertEqual(v_node.acl, ['acl1'])
        self.assertEqual(v_node.neighbors, ['analyte1'])
        self.assertIsNotNone(v_node.versioned)
        self.assertIsNotNone(v_node.key)

    def test_versions_property(self):
        with self.g.session_scope() as session:
            portion = self.new_portion()
            analyte = self.new_analyte()
            portion.analytes = [analyte]
            session.add(portion)

        with self.g.session_scope() as session:
            portion = self.g.nodes(md.Portion).one()
            v_node = md.VersionedNode.clone(portion)
            session.add(v_node)

        with self.g.session_scope():
            portion = self.g.nodes(md.Portion).one()
            portion._versions.one()

        with self.assertRaises(RuntimeError):
            portion._versions.one()

        with self.g.session_scope() as s:
            portion.get_versions(s).one()
