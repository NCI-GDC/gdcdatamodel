import os
import json
from avro.schema import make_avsc_object, Names

package_dir = os.path.dirname(os.path.realpath(__file__))
schema_src_dir = os.path.join(package_dir, 'avro', 'schemata')

node_schema_file_list = [os.path.join(schema_src_dir, f) for f in [
    "field_types.avsc",
    "node_labels.avsc",
    "node_properties.avsc",
    "nodes.avsc"
]]

edge_schema_file_list = [os.path.join(schema_src_dir, f) for f in [
    "schemata/src/node_labels.avsc",
    "schemata/src/edge_labels.avsc",
    "schemata/src/node_pairs.avsc",
    "schemata/src/edges.avsc"
]]


def make_avsc_object_from_file(path, names=None):
    """Loads a schema from avsc file into avsc_object

    :param file_path: path to schema file
    :param names: avro.schema.Names object
    :returns: avsc_object
    """

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
node_avsc_object = make_avsc_object_from_file_list(node_schema_file_list)
edge_avsc_object = make_avsc_object_from_file_list(node_schema_file_list)
