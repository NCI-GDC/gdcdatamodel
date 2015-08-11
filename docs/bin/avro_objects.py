import os
import json
import logging
from avro.schema import make_avsc_object, Names

logging.debug('loading gdcdatamodel avro schema...')
print __file__
package_dir = os.path.dirname(os.path.realpath(__file__))
print package_dir
schema_src_dir = os.path.join(package_dir, '../../gdcdatamodel', 'avro', 'schemata')
print schema_src_dir

node_schema_file_list = [os.path.join(schema_src_dir, f) for f in [
    "field_types.avsc",
    "node_labels.avsc",
    "node_properties.avsc",
    "nodes.avsc",
]]

edge_schema_file_list = [os.path.join(schema_src_dir, f) for f in [
    "field_types.avsc",
    "node_labels.avsc",
    "edge_labels.avsc",
    "node_pairs.avsc",
    "edge_properties.avsc",
    "edges.avsc",
]]


def make_avsc_object_from_file(path, names=None):
    """Loads a schema from avsc file into avsc_object
    :param file_path: path to schema file
    :param names: avro.schema.Names object
    :returns: avsc_object
    """

    logging.debug('Loading avsc from file {}'.format(path))
    with open(path, 'r') as f:
        schema = json.loads(f.read())
    avsc_object = make_avsc_object(schema, names)
    return avsc_object


def make_avsc_object_from_file_list(file_list):
    """Compiles a schema from list of avsc files into avsc_object
    :param list(str) file_list:
        A list containing the absolute path to the files from which
        the avro schema is to be generated
    """

    known_schemata = Names()
    for path in file_list:
        avsc_object = make_avsc_object_from_file(path, known_schemata)
    return avsc_object


# Create avsc objects
logging.debug('Loading gdcdatamodel avro schema.')
node_avsc_object = make_avsc_object_from_file_list(node_schema_file_list)
edge_avsc_object = make_avsc_object_from_file_list(edge_schema_file_list)
node_avsc_json = node_avsc_object.to_json()
edge_avsc_json = edge_avsc_object.to_json()
logging.debug('gdcdatamodel avro schema loaded.')