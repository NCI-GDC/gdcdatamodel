from gdcdatamodel import node_avsc_json
from mapped_entities import (
    file_tree, file_traversal,
    participant_tree, participant_traversal,
    annotation_tree, annotation_traversal,
    project_tree, annotation_tree,
    ONE_TO_MANY, ONE_TO_ONE
)


def _get_es_type(_type):
    if 'long' in _type or 'int' in _type:
        return 'long'
    else:
        return 'string'


def _munge_properties(source):
    a = [n['fields'] for n in node_avsc_json if n['name'] == source]
    if not a:
        return
    fields = [b['type'] for b in a[0] if b['name'] == 'properties']
    fields[0][0]['fields'].append({
        'name': '{}_id'.format(source),
        'type': 'string'
    })
    return {b['name']: {
        'type': _get_es_type(b['type']),
        'index': 'not_analyzed'
    } for b in fields[0][0]['fields']}


def _walk_tree(tree, mapping):
    for k, v in [(k, v) for k, v in tree.items() if k != 'corr']:
        corr, name = v['corr']
        mapping[name] = {'properties': _munge_properties(k)}
        _walk_tree(tree[k], mapping[name])
    return mapping


def get_file_es_mapping(include_participant=True):
    files = {"_id": {"path": "file_id"}}
    files["properties"] = _walk_tree(file_tree, _munge_properties("file"))
    if include_participant:
        files["properties"]['participant'] = get_participant_es_mapping(False)
        files["properties"]["participant"]["type"] = "nested"
    return files


def get_participant_es_mapping(include_file=True):
    participant = {"_id": {"path": "participant_id"}}
    participant["properties"] = _walk_tree(
        participant_tree, _munge_properties("participant"))
    if include_file:
        participant["properties"]['files'] = get_file_es_mapping(True)
        participant["properties"]["files"]["type"] = "nested"
    participant["properties"]["summary"] = {"properties": {
        "file_count": {u'index': u'not_analyzed', u'type': u'long'},
        "file_size": {u'index': u'not_analyzed', u'type': u'long'},
        "experimental_strategies": {"properties": {
            "experimental_strategy": {u'index': u'not_analyzed', u'type': u'string'},
            "file_count": {u'index': u'not_analyzed', u'type': u'long'},
        }},
        "data_types": {"properties": {
            "data_type": {u'index': u'not_analyzed', u'type': u'string'},
            "file_count": {u'index': u'not_analyzed', u'type': u'long'},
        }},
    }}
    return participant


def get_annotation_es_mapping(include_file=True):
    annotation = _walk_tree(annotation_tree, _munge_properties("annotation"))
    annotation["_id"] = {"path": "annotation_id"}
    if include_file:
        annotation['files'] = get_file_es_mapping(False)
        annotation["files"]["type"] = "nested"
    return annotation


def get_project_es_mapping():
    project = {"_id": {"path": "project_id"}}
    project["properties"] = _walk_tree(
        project_tree, _munge_properties("project"))
    project["properties"]["summary"] = {"properties": {
        "data_file_count": {u'index': u'not_analyzed', u'type': u'long'},
        "file_size": {u'index': u'not_analyzed', u'type': u'long'},
        "participant_count": {u'index': u'not_analyzed', u'type': u'long'},
        "experimental_strategies": {"properties": {
            "participant_count": {u'index': u'not_analyzed', u'type': u'long'},
            "experimental_strategy": {u'index': u'not_analyzed', u'type': u'string'},
            "file_count": {u'index': u'not_analyzed', u'type': u'long'},
        }},
        "data_types": {"properties": {
            "participant_count": {u'index': u'not_analyzed', u'type': u'long'},
            "data_type": {u'index': u'not_analyzed', u'type': u'string'},
            "file_count": {u'index': u'not_analyzed', u'type': u'long'},
        }},
    }}
    return project
