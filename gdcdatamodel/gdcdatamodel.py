import os
import re
import json
import logging
from avro.schema import make_avsc_object, Names

logging.debug('loading gdcdatamodel avro schema...')
package_dir = os.path.dirname(os.path.realpath(__file__))
schema_src_dir = os.path.join(package_dir, 'avro', 'schemata')

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

from pprint import pprint


def get_edge_maps():
    """Returns a map from edges to destinations

    """
    p = re.compile("(([a-z_]+):([a-z_]+))")
    edge_map_forward, edge_map_backward = {}, {}
    for match in p.findall(str(edge_avsc_json)):
        if match[1] not in edge_map_forward:
            edge_map_forward[match[1]] = []
        if match[2] not in edge_map_backward:
            edge_map_backward[match[2]] = []
        edge_map_forward[match[1]].append(match[2])
        edge_map_backward[match[2]].append(match[1])
    return edge_map_forward, edge_map_backward


def get_es_type(_type):
    if 'long' in _type or 'int' in _type:
        return 'long'
    else:
        return 'string'


def _munge_properties(source):
    a = [n['fields'] for n in node_avsc_json if n['name'] == source][0]
    fields = [b['type'] for b in a if b['name'] == 'properties']
    return {
        b['name']: {
            'type': get_es_type(b['type']),
            'index': 'not_analyzed',
        } for b in fields[0][0]['fields']
    }


def _walk_prop_edges(source, edge_map, graph, level=0, includes=[]):
    # graph['properties'] = _munge_properties(source)
    for dst in edge_map.get(source, []):
        if dst not in [source, 'file'] and (not includes or dst in includes):
            graph[dst] = {}
            _walk_prop_edges(dst, edge_map, graph[dst], level+1)
    return graph


def _get_participant_es_mapping_base():
    """Generates the elasticsearch mapping for participants from the avro
    schema

    """
    forward, backward = get_edge_maps()
    return _walk_prop_edges(
        'participant', forward, _walk_prop_edges(
            'participant', backward, {}))


def get_file_es_mapping():
    forward, backward = get_edge_maps()
    includes = [
        'annotation', 'data_subtype', 'platform', 'experimental_strategy',
        'data_format', 'tag', 'archive', 'center']
    mapping = _walk_prop_edges(
        'file', backward, _walk_prop_edges(
            'file', forward, {}, includes=includes))
    mapping['participant'] = _get_participant_es_mapping_base()
    return {'file': mapping}


def get_participant_es_mapping():
    mapping = _get_participant_es_mapping_base()
    mapping['file'] = get_file_es_mapping()['file']
    return {'participant': mapping}
