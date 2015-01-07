import unittest
import logging
import json
import os
from avro.io import validate
from gdcdatamodel import node_avsc_object


logging.basicConfig(level=logging.INFO)


class TestAvroSchemaValidation(unittest.TestCase):

    def setUp(self):
        self.test_dir = os.path.dirname(os.path.realpath(__file__))
        self.example_node_dir = os.path.join(
            os.path.abspath(os.path.join(self.test_dir, os.pardir)),
            'examples',
            'nodes',
        )

    def test_valid_aliquot_node(self):
        valid_path = os.path.join(
            self.example_node_dir, 'aliquot_valid.json')
        with open(valid_path, 'r') as f:
            valid_aliquot_json = json.loads(f.read())
        print json.dumps(json.loads(str(node_avsc_object)), indent=2)
        self.assertTrue(validate(node_avsc_object, valid_aliquot_json))

    def test_invalid_aliquot_node(self):
        invalid_path = os.path.join(
            self.example_node_dir, 'aliquot_invalid.json')
        with open(invalid_path, 'r') as f:
            invalid_aliquot_json = json.loads(f.read())
        self.assertFalse(validate(node_avsc_object, invalid_aliquot_json))
