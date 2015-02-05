from gdcdatamodel import node_avsc_json
from mapped_entities import\
    file_tree, file_traversal,\
    participant_tree, participant_traversal,\
    ONE_TO_MANY, ONE_TO_ONE, annotation_tree


def _get_es_type(_type):
    if 'long' in _type or 'int' in _type:
        return 'long'
    else:
        return 'string'


def _munge_properties(source):
    a = [n['fields'] for n in node_avsc_json if n['name'] == source][0]
    fields = [b['type'] for b in a if b['name'] == 'properties']
    fields[0][0]['fields'].append({'name': 'uuid', 'type': 'string'})
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
    files = _walk_tree(file_tree, {})
    if include_participant:
        files['participant'] = get_participant_es_mapping(False)
    return files


def get_participant_es_mapping(include_file=True):
    participant = _walk_tree(file_tree, {})
    if include_file:
        participant['files'] = get_file_es_mapping(False)
    return participant


def get_annotation_es_mapping(include_file=True):
    annotation = _walk_tree(file_tree, {})
    if include_file:
        annotation['files'] = get_file_es_mapping(False)
    return annotation