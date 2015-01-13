import logging
import unittest
import os
from zug.datamodel import xml2psqlgraph, latest_urls, extract_tar
from zug.datamodel.import_tcga_code_tables import \
    import_center_codes, import_tissue_source_site_codes
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from gdcdatamodel import node_avsc_object, edge_avsc_object

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

test_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(os.path.abspath(
    os.path.join(test_dir, os.path.pardir)), 'data')
mapping = os.path.join(data_dir, 'bcr.yaml')
center_csv_path = os.path.join(data_dir, 'centerCode.csv')
tss_csv_path = os.path.join(data_dir, 'tissueSourceSite.csv')

datatype = 'biospecimen'
host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'


def initialize(validated=False):

    if validated:
        node_validator = AvroNodeValidator(node_avsc_object)
        edge_validator = AvroEdgeValidator(edge_avsc_object)
    else:
        node_validator, edge_validator = None, None

    parser = latest_urls.LatestURLParser(
        constraints={'data_level': 'Level_1', 'platform': 'bio'},
        url_key='dcc_archive_url',
    )
    extractor = extract_tar.ExtractTar(
        regex=".*(bio).*(Level_1).*\\.xml"
    )
    converter = xml2psqlgraph.xml2psqlgraph(
        translate_path=mapping,
        data_type=datatype,
        host=host,
        user=user,
        password=password,
        database=database,
        edge_validator=edge_validator,
        node_validator=node_validator,
        ignore_missing_properties=True,
    )
    return parser, extractor, converter


class TestTCGABiospeceminImport(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        self.parser, self.extrator, self.converter = initialize()

    def tearDown(self):
        with self.converter.graph.engine.begin() as conn:
            conn.execute('delete from edges')
            conn.execute('delete from nodes')
            conn.execute('delete from voided_edges')
            conn.execute('delete from voided_nodes')
        self.converter.graph.engine.dispose()

    def test_convert_sample(self):
        with open(os.path.join(data_dir, 'sample.xml')) as f:
            xml = f.read()
        self.converter.xml2psqlgraph(xml)
        self.converter.export()

    def test_convert_validate_nodes_sample(self):
        import_center_codes(self.converter.graph, center_csv_path)
        import_tissue_source_site_codes(self.converter.graph, tss_csv_path)
        self.converter.export_nodes()
        with open(os.path.join(data_dir, 'sample.xml')) as f:
            xml = f.read()
        self.converter.xml2psqlgraph(xml)
        self.converter.export_nodes()

    def test_convert_validate_edges_sample(self):
        import_center_codes(self.converter.graph, center_csv_path)
        import_tissue_source_site_codes(self.converter.graph, tss_csv_path)
        self.converter.export_nodes()
        with open(os.path.join(data_dir, 'sample.xml')) as f:
            xml = f.read()
        self.converter.xml2psqlgraph(xml)
        self.converter.export_nodes()
        self.converter.export_edges()

    def test_versioned_idempotency(self):
        import_center_codes(self.converter.graph, center_csv_path)
        import_tissue_source_site_codes(self.converter.graph, tss_csv_path)
        g = self.converter.graph

        ignored_labels = ['center', 'tissue_source_site']
        self.converter.export_nodes()
        with open(os.path.join(data_dir, 'sample.xml')) as f:
            xml = f.read()

        self.converter.xml2psqlgraph(xml)
        self.converter.export(group_id='group1', version=1)
        v1 = {n.node_id: n for n in g.get_nodes().all()
              if n.label not in ignored_labels}

        self.converter.xml2psqlgraph(xml)
        self.converter.export(group_id='group1', version=2.5)
        v2 = {n.node_id: n for n in g.get_nodes().all()
              if n.label not in ignored_labels}

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
        import_center_codes(self.converter.graph, center_csv_path)
        import_tissue_source_site_codes(self.converter.graph, tss_csv_path)
        self.converter.export_nodes()

        ignored_labels = ['center', 'tissue_source_site']
        g = self.converter.graph

        with open(os.path.join(data_dir, 'sample.xml')) as f:
            xml = f.read()
        self.converter.xml2psqlgraph(xml)
        self.converter.export(group_id='group1', version=1)
        v1 = {n.node_id: n for n in g.get_nodes().all()
              if n.label not in ignored_labels}

        with open(os.path.join(data_dir, 'sample_v2.xml')) as f:
            xml = f.read()
        self.converter.xml2psqlgraph(xml)
        self.converter.export(group_id='group1', version=2.5)
        v2 = {n.node_id: n for n in g.get_nodes().all()
              if n.label not in ignored_labels}

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
