import logging
import os
import unittest
import yaml
from gdcdatamodel import models
from psqlgraph import Edge, Node

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

    def test_convert_sample1(self):
        # convert sample data
        self.converter.xml2psqlgraph(self.xml)
        self.converter.export()

        # test conversion for accuracy
        with self.converter.graph.session_scope():
            first = self.converter.graph.node_lookup_one(node_id='level1')
            second = self.converter.graph.node_lookup_one(node_id='level2')
            third = self.converter.graph.node_lookup_one(node_id='level3')
        self.assertEqual(first.properties['submitter_id'], '1a')
        self.assertEqual(second.properties['sample_type_id'], '2a')
        self.assertEqual(second.properties['sample_type'], '2b')
        self.assertEqual(third.properties['submitter_id'], '3a')
        self.assertEqual(third.properties['creation_datetime'], 3)
