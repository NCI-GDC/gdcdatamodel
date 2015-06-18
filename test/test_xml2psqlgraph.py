import os
import yaml

from gdcdatamodel import models
import base
from zug import datamodel


class TestXML2PsqlGraph(base.ZugsSimpleTestBase):

    def setUp(self):
        super(TestXML2PsqlGraph, self).setUp()
        # load sample data
        with open(os.path.join(base.TEST_DIR, 'sample1.yaml')) as f:
            xml_mapping = yaml.load(f.read())
        self.converter = datamodel.xml2psqlgraph.xml2psqlgraph(
            xml_mapping=xml_mapping, **self.graph_info)
        with open(os.path.join(base.TEST_DIR, 'sample1.xml')) as f:
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
        self.assertEqual(second.properties['sample_type_id'], '02')
        self.assertEqual(second.properties['sample_type'], 'DNA')
        self.assertEqual(third.properties['submitter_id'], '3a')
        self.assertEqual(third.properties['creation_datetime'], 3)
