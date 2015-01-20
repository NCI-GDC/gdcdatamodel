import logging
import os
import unittest
import yaml

from zug import datamodel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
test_dir = os.path.dirname(os.path.realpath(__file__))

settings = dict(
    host='localhost',
    user='test',
    password='test',
    database='automated_test',
)

logging.basicConfig(level=logging.INFO)


class TestXML2PsqlGraph(unittest.TestCase):

    def setUp(self):
        # load sample data
        with open(os.path.join(test_dir, 'sample1.yaml')) as f:
            xml_mapping = yaml.load(f.read())
        self.converter = datamodel.xml2psqlgraph.xml2psqlgraph(
            xml_mapping=xml_mapping, **settings)
        with open(os.path.join(test_dir, 'sample1.xml')) as f:
            self.xml = f.read()

    def tearDown(self):
        with self.converter.graph.engine.begin() as conn:
            conn.execute('delete from edges')
            conn.execute('delete from nodes')
            conn.execute('delete from voided_edges')
            conn.execute('delete from voided_nodes')
        self.converter.graph.engine.dispose()

    def test_convert_sample1(self):
        # convert sample data
        self.converter.xml2psqlgraph(self.xml)
        self.converter.export()

        # test conversion for accuracy
        first = self.converter.graph.node_lookup_one(node_id='level1')
        second = self.converter.graph.node_lookup_one(node_id='level2')
        third = self.converter.graph.node_lookup_one(node_id='level3')
        self.assertEqual(first.properties['text1.1'], '1a')
        self.assertEqual(second.properties['text2.2'], '2a')
        self.assertEqual(second.properties['text2.3'], '2b')
        self.assertEqual(third.properties['text3.1'], '3a')
        self.assertEqual(third.properties['text3.2'], '3b')
