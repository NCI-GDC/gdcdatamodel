import unittest
import logging
import os

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

TEST_DIR = os.path.dirname(os.path.abspath(__file__))


class TestXML2PsqlGraph(unittest.TestCase):

    def test_convert_sample1(self):

        # load sample data
        converter = datamodel.xml2psqlgraph.xml2psqlgraph(
            translate_path=os.path.join(TEST_DIR, 'sample1.yaml'), **settings)
        with open(os.path.join(TEST_DIR, 'sample1.xml')) as f:
            xml = f.read()

        # convert sample data
        converter.xml2psqlgraph(xml)
        converter.export()

        # test conversion for accuracy
        first = converter.graph.node_lookup_one(node_id='level1')
        second = converter.graph.node_lookup_one(node_id='level2')
        third = converter.graph.node_lookup_one(node_id='level3')
        self.assertEqual(first.properties['text1.1'], '1a')
        self.assertEqual(second.properties['text2.2'], '2a')
        self.assertEqual(second.properties['text2.3'], '2b')
        self.assertEqual(third.properties['text3.1'], '3a')
        self.assertEqual(third.properties['text3.2'], '3b')
