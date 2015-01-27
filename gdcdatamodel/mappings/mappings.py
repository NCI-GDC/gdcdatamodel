from gdcdatamodel import node_avsc_json
from mapped_entities import file_tree, participant_tree, annotation_tree


def _get_es_type(_type):
    if 'long' in _type or 'int' in _type:
        return 'long'
    else:
        return 'string'


def _munge_properties(source):
    a = [n['fields'] for n in node_avsc_json if n['name'] == source][0]
    fields = [b['type'] for b in a if b['name'] == 'properties']
    return {b['name']: {
        'type': _get_es_type(b['type']),
        'index': 'not_analyzed'
    } for b in fields[0][0]['fields']}


def _walk_tree(tree, mapping):
    for branch in tree:
        mapping[branch] = {
            # 'properties': _munge_properties(branch)
        }
        _walk_tree(tree[branch], mapping[branch])
    return mapping


def get_file_es_mapping(include_participant=True):
    return _walk_tree(
        file_tree, ({
            'participant': _walk_tree(
                participant_tree,
                get_participant_es_mapping(False))})
        if include_participant else {})


def get_participant_es_mapping(include_file=True):
    return _walk_tree(
        participant_tree, ({
            'file': _walk_tree(
                file_tree,
                get_file_es_mapping())})
        if include_file else {})


def get_annotation_es_mapping(include_file=True):
    return _walk_tree(
        annotation_tree, ({
            'file': _walk_tree(
                file_tree,
                get_file_es_mapping(False))})
        if include_file else {})
