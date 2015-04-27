import logging
import unittest
import os
from zug.datamodel import xml2psqlgraph, extract_tar, bcr_xml_mapping
from zug.datamodel.prelude import create_prelude_nodes
from psqlgraph import Node, Edge

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

data_dir = os.path.dirname(os.path.realpath(__file__))

datatype = 'biospecimen'
host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'


def initialize():

    extractor = extract_tar.ExtractTar(
        regex=".*(bio).*(Level_1).*\\.xml"
    )
    converter = xml2psqlgraph.xml2psqlgraph(
        xml_mapping=bcr_xml_mapping,
        host=host,
        user=user,
        password=password,
        database=database,
    )
    return extractor, converter


class TestTCGABiospeceminImport(unittest.TestCase):

    IGNORED_LABELS = [
        'center', 'tissue_source_site', 'tag', 'experimental_strategy',
        'platform', 'data_subtype', 'data_type', 'program', 'project',
        'data_format'
    ]

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        self.extrator, self.converter = initialize()
        create_prelude_nodes(self.converter.graph)

    def tearDown(self):
        self.clear_tables()

    def clear_tables(self):
        with self.converter.graph.engine.begin() as conn:
            for table in Node().get_subclass_table_names():
                if table != Node.__tablename__:
                    conn.execute('delete from {}'.format(table))
            for table in Edge().get_subclass_table_names():
                if table != Edge.__tablename__:
                    conn.execute('delete from {}'.format(table))
            conn.execute('delete from _voided_nodes')
            conn.execute('delete from _voided_edges')
        self.converter.graph.engine.dispose()

    def test_convert_sample(self):
        with open(os.path.join(data_dir, 'sample_biospecimen.xml')) as f:
            xml = f.read()
        self.converter.xml2psqlgraph(xml)
        self.converter.export_nodes(group_id='group1', version=1)

    def test_convert_validate_nodes_sample(self):
        self.converter.export_nodes(group_id='group1', version=1)
        with open(os.path.join(data_dir, 'sample_biospecimen.xml')) as f:
            xml = f.read()
        self.converter.xml2psqlgraph(xml)
        self.converter.export_nodes(group_id='group1', version=1)

    def test_convert_validate_edges_sample(self):
        self.converter.export_nodes(group_id='group1', version=1)
        with open(os.path.join(data_dir, 'sample_biospecimen.xml')) as f:
            xml = f.read()
        self.converter.xml2psqlgraph(xml)
        self.converter.export_nodes(group_id='group1', version=1)
        self.converter.export_edges()

    def test_versioned_idempotency(self):
        g = self.converter.graph
        self.converter.export_nodes(group_id='group1', version=1)
        with open(os.path.join(data_dir, 'sample_biospecimen.xml')) as f:
            xml = f.read()

        self.converter.xml2psqlgraph(xml)
        self.converter.export(group_id='group1', version=1)
        with g.session_scope():
            v1 = {n.node_id: n for n in g.get_nodes().all()
                  if n.label not in self.IGNORED_LABELS}

        self.converter.xml2psqlgraph(xml)
        self.converter.export(group_id='group1', version=2.5)
        with g.session_scope():
            v2 = {n.node_id: n for n in g.get_nodes().all()
                  if n.label not in self.IGNORED_LABELS}

        for node_id, node in v1.iteritems():
            self.assertTrue(node_id in v2)
            self.assertTrue(v1[node_id].system_annotations == {
                'group_id': 'group1', 'version': 1})
            self.assertEqual(v1[node_id].properties, v2[node_id].properties)

        for node_id, node in v2.iteritems():
            self.assertTrue(node_id in v1)
            self.assertTrue(v2[node_id].system_annotations == {
                'group_id': 'group1', 'version': 2.5})
            self.assertEqual(v2[node_id].properties, v1[node_id].properties)

    def test_versioned_import(self):
        self.converter.export_nodes()

        g = self.converter.graph

        with open(os.path.join(data_dir, 'sample_biospecimen.xml')) as f:
            xml = f.read()
        self.converter.xml2psqlgraph(xml)
        self.converter.export(group_id='group1', version=1)
        with g.session_scope():
            v1 = {n.node_id: n for n in g.get_nodes().all()
                  if n.label not in self.IGNORED_LABELS}

        with open(os.path.join(data_dir, 'sample_biospecimen_v2.xml')) as f:
            xml = f.read()
        self.converter.xml2psqlgraph(xml)
        self.converter.export(group_id='group1', version=2.5)
        with g.session_scope():
            v2 = {n.node_id: n for n in g.get_nodes().all()
                  if n.label not in self.IGNORED_LABELS}

        for node_id, node in v1.iteritems():
            self.assertTrue(node_id in v2)
            self.assertTrue(v1[node_id].system_annotations == {
                'group_id': 'group1', 'version': 1})

        for node_id, node in v2.iteritems():
            self.assertTrue(node_id in v1)
            self.assertTrue(v2[node_id].system_annotations == {
                'group_id': 'group1', 'version': 2.5})
            if v1[node_id].node_id != '5fa9998b-deff-493e-8a8e-dc2422192a48':
                self.assertEqual(
                    v1[node_id].properties, v2[node_id].properties)
            else:
                self.assertFalse(v1[node_id]['is_ffpe'])
                self.assertTrue(v2[node_id]['is_ffpe'])
