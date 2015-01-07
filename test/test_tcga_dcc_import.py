import logging
import unittest
import os
from zug.datamodel import xml2psqlgraph, latest_urls, extract_tar
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from gdcdatamodel import node_avsc_object, edge_avsc_object

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
test_dir = os.path.dirname(os.path.realpath(__file__))

mapping = os.path.join(test_dir, 'bcr.yaml')
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


class TestTCGAImport(unittest.TestCase):

    def test_convert_sample(self):
        parser, extractor, converter = initialize()
        with open(os.path.join(test_dir, 'sample.xml')) as f:
            xml = f.read()
        converter.xml2psqlgraph(xml)
        converter.export()

    def test_convert_validate_sample(self):
        parser, extractor, converter = initialize(validated=True)
        converter.graph.engine.execute('delete from edges')
        converter.graph.engine.execute('delete from nodes')
        with open(os.path.join(test_dir, 'sample.xml')) as f:
            xml = f.read()
        converter.xml2psqlgraph(xml)
        converter.export()
