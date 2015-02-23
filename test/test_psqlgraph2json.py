import logging
import unittest
import os
from zug.datamodel import xml2psqlgraph
from zug.datamodel.prelude import create_prelude_nodes
from zug.datamodel import xml2psqlgraph, latest_urls, bcr_xml_mapping
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from gdcdatamodel import node_avsc_object, edge_avsc_object
from psqlgraph import PsqlGraphDriver

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

data_dir = os.path.dirname(os.path.realpath(__file__))

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'

converter = xml2psqlgraph.xml2psqlgraph(
    xml_mapping=bcr_xml_mapping,
    host=host,
    user=user,
    password=password,
    database=database,
    node_validator=AvroNodeValidator(node_avsc_object),
    edge_validator=AvroEdgeValidator(edge_avsc_object),
)
g = PsqlGraphDriver(
    host=host,
    user=user,
    password=password,
    database=database,
    node_validator=AvroNodeValidator(node_avsc_object),
    edge_validator=AvroEdgeValidator(edge_avsc_object),
)


class TestTCGABiospeceminImport(unittest.TestCase):

    def setUp(self):
        create_prelude_nodes(g)
        converter.export_nodes()
        with open(os.path.join(data_dir, 'sample_biospecimen.xml')) as f:
            xml = f.read()
        converter.xml2psqlgraph(xml)
        converter.export_nodes()

    def tearDown(self):
        with g.engine.begin() as conn:
            conn.execute('delete from edges')
            conn.execute('delete from nodes')
            conn.execute('delete from voided_edges')
            conn.execute('delete from voided_nodes')
        g.engine.dispose()

    def test_denormalize_participant(self):
        with g.session_scope():
            print g.nodes().count()

        raise Exception()
