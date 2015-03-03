from gdcdatamodel import node_avsc_json
import re
from mapped_entities import (
    file_tree, file_traversal,
    participant_tree, participant_traversal,
    annotation_tree, annotation_traversal,
    project_tree, annotation_tree,
    ONE_TO_MANY, ONE_TO_ONE
)


MULTIFIELDS = re.compile("|".join([
    "submitter_id", "name", "code", "primary_site",
    "disease_type", "name", "file_name",
]))


FLATTEN = ['tag', 'platform', 'data_format', 'experimental_strategy']

"""
Logic for multifields
1) no nested entries
2) top levels that match MULTIFIELDS above

"""


def index_settings():
    return {"settings":
            {"analysis":
             {"analyzer":
              {"id_search":
               {"tokenizer": "whitespace",
                "filter": ["lowercase"],
                "type": "custom"},
               "id_index": {
                   "tokenizer": "whitespace",
                   "filter": [
                       "lowercase",
                       "edge_ngram"
                   ],
                   "type": "custom"
               }},
              "filter": {
                  "edge_ngram": {
                      "side": "front",
                      "max_gram": 20,
                      "min_gram": 2,
                      "type": "edge_ngram"
                  }}}}}


def _get_header():
    return {
        "dynamic": "strict",
        "_all": {
            "enabled": False
        },
        "_source": {
            "compress": True,
            "excludes": ["__comment__"]
        },
    }


def _get_es_type(_type):
    if 'long' in _type or 'int' in _type:
        return 'long'
    else:
        return 'string'


def _munge_properties(source, nested=True):

    # Get properties from schema
    a = [n['fields'] for n in node_avsc_json if n['name'] == source]
    if not a:
        return
    fields = [b['type'] for b in a[0] if b['name'] == 'properties']

    # Add id to document
    id_name = '{}_id'.format(source)
    if nested:
        doc = {id_name: {'index': 'not_analyzed', 'type': 'string'}}
    else:
        doc = _multfield_template(id_name)

    # Add all properties to document
    for field in fields[0][0]['fields']:
        name = field['name']
        if not nested and MULTIFIELDS.match(name):
            doc.update(_multfield_template(name))
        else:
            _type = _get_es_type(
                [c['type'] if isinstance(c['type'], (unicode, str))
                 else 'string' for c in b['type'][0]['fields']
                 if c['name'] == name])
            doc[name] = {'type': _type}
            if str(_type) == 'string':
                doc[name]['index'] = 'not_analyzed'
    return doc


def _multfield_template(name):
    return {name: {
        "type": "string", "fields": {
            "raw": {
                "index": "not_analyzed",
                "store": "yes",
                "type": "string"
            }, "analyzed": {
                "index": "analyzed",
                "index_analyzer": "id_index",
                "search_analyzer": "id_search",
                "type": "string"
            }, "search": {
                "index": "analyzed",
                "analyzer": "id_search",
                "type": "string"
            }}}}


def _walk_tree(tree, mapping):
    for k, v in [(k, v) for k, v in tree.items() if k != 'corr']:
        corr, name = v['corr']
        if name not in mapping:
            mapping[name] = {'properties': {}}
        if k in FLATTEN:
            mapping.update(_multfield_template(name))
        elif k == 'annotation':
            mapping['annotations'] = annotation_body()
            mapping['annotations']['type'] = 'nested'
        else:
            nested = (corr == ONE_TO_MANY)
            mapping[name]['properties'].update(
                _munge_properties(k, nested))
            _walk_tree(tree[k], mapping[name]['properties'])
            if nested:
                mapping[name]['type'] = 'nested'
    return mapping


def flatten_data_type(root):
    root.pop('data_subtype', None)
    root.update(_multfield_template('data_subtype'))
    root.update(_multfield_template('data_type'))


def get_file_es_mapping(include_participant=True):
    files = _get_header()
    files["_id"] = {"path": "file_id"}
    files["properties"] = _walk_tree(file_tree, _munge_properties("file"))
    flatten_data_type(files['properties'])

    # Related files/archives
    files["properties"]['related_files'] = {
        'type': 'nested',
        'properties': _munge_properties("file")}
    files['properties']['related_files']['properties']['data_type'] = (
        _munge_properties("file"))
    files["properties"]['related_archives'] = {
        'type': 'nested',
        'properties': _munge_properties("archive")}

    # Temporary until datetimes are backported
    files['properties']['uploaded_datetime'] = {'type': 'long'}
    files['properties']['published_datetime'] = {'type': 'long'}

    # File access
    files['properties']['access'] = {'index': 'not_analyzed', 'type': 'string'}
    files['properties']['acl'] = {'index': 'not_analyzed', 'type': 'string'}

    # Participant
    files["properties"].pop('participant', None)
    if include_participant:
        files['type'] = 'nested'
        files["properties"]["participants"] = get_participant_es_mapping(False)
        files["properties"]["participants"]["type"] = "nested"
    return files


def get_participant_es_mapping(include_file=True):
    participant = _get_header()
    participant["_id"] = {"path": "participant_id"}
    participant["properties"] = _walk_tree(
        participant_tree, _munge_properties("participant"))
    participant["properties"].pop('file', None)
    participant['properties']['metadata_files'] = {
        'type': 'nested',
        'properties': _munge_properties("file"),
    }
    participant['properties']['acl'] = {'index': 'not_analyzed', 'type': 'string'}
    if include_file:
        participant["properties"]['files'] = get_file_es_mapping(True)
        participant["properties"]["files"]["type"] = "nested"
    participant["properties"]["summary"] = {"properties": {
        "file_count": {u'index': u'not_analyzed', u'type': u'long'},
        "file_size": {u'index': u'not_analyzed', u'type': u'long'},
        "experimental_strategies": {"type": "nested", "properties": {
            "experimental_strategy": {u'index': u'not_analyzed', u'type': u'string'},
            "file_count": {u'index': u'not_analyzed', u'type': u'long'},
        }},
        "data_types": {"type": "nested", "properties": {
            "data_type": {u'index': u'not_analyzed', u'type': u'string'},
            "file_count": {u'index': u'not_analyzed', u'type': u'long'},
        }},
    }}
    return participant


def annotation_body(nested=True):
    annotation = {}
    annotation["properties"] = _munge_properties("annotation", nested)
    annotation["properties"]["item_type"] = {
        'index': 'not_analyzed', 'type': 'string'}
    annotation["properties"]["item_id"] = {
        'index': 'not_analyzed', 'type': 'string'}
    return annotation


def get_annotation_es_mapping(include_file=True):
    annotation = _get_header()
    annotation["_id"] = {"path": "annotation_id"}
    annotation.update(annotation_body(nested=False))
    annotation["properties"].update(_multfield_template('item_id'))
    annotation['properties'].update({
        'project': {'properties': _munge_properties("project")}})
    annotation['properties']['project']['properties']['program'] = (
        {'properties': _munge_properties("program")})
    return annotation


def get_project_es_mapping():
    project = _get_header()
    project["_id"] = {"path": "project_id"}
    project["properties"] = _walk_tree(
        project_tree, _munge_properties("project"))
    project["properties"]["summary"] = {"properties": {
        "file_count": {u'index': u'not_analyzed', u'type': u'long'},
        "file_size": {u'index': u'not_analyzed', u'type': u'long'},
        "participant_count": {u'index': u'not_analyzed', u'type': u'long'},
        "experimental_strategies": {"properties": {
            "participant_count": {u'index': u'not_analyzed', u'type': u'long'},
            "experimental_strategy": {u'index': u'not_analyzed', u'type': u'string'},
            "file_count": {u'index': u'not_analyzed', u'type': u'long'},
        }, 'type': 'nested'},
        "data_types": {"properties": {
            "participant_count": {u'index': u'not_analyzed', u'type': u'long'},
            "data_type": {u'index': u'not_analyzed', u'type': u'string'},
            "file_count": {u'index': u'not_analyzed', u'type': u'long'},
        }, 'type': 'nested'},
    }}
    return project
