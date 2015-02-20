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
    "disease_type", "project_name"
]))


def index_settings():
    return {"settings": {"analysis": {"analyzer": {"id_search":
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
                                      "filter": {"edge_ngram": {
                                          "side": "front",
                                          "max_gram": 20,
                                          "min_gram": 2,
                                          "type": "edge_ngram"}}}}}


def _get_header():
    return {
        "dynamic": "true",
        "dynamic_templates": [{
            "template_1": {
                "mapping": {
                    "index": "not_analyzed"
                },
                "match": "*",
                "match_mapping_type": "string"
            }
        }],
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


def _munge_properties(source):
    a = [n['fields'] for n in node_avsc_json if n['name'] == source]
    if not a:
        return
    fields = [b['type'] for b in a[0] if b['name'] == 'properties']
    doc = _multfield_template('{}_id'.format(source))
    for field in fields[0][0]['fields']:
        name = field['name']
        if MULTIFIELDS.match(name):
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
    # return {name: {'type': 'string', 'index': 'not_analyzed'}}
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
        mapping[name] = {'properties': _munge_properties(k)}
        _walk_tree(tree[k], mapping[name])
    return mapping


def get_file_es_mapping(include_participant=True):
    files = _get_header()
    files["_id"] = {"path": "file_id"}
    files["properties"] = _walk_tree(file_tree, _munge_properties("file"))
    files["properties"]['related_files'] = {
        'type': 'nested',
        'properties': _munge_properties("file"),
    }
    if include_participant:
        files["properties"]['participant'] = get_participant_es_mapping(False)
        files["properties"]["participant"]["type"] = "nested"
    return files


def get_participant_es_mapping(include_file=True):
    participant = _get_header()
    participant["_id"] = {"path": "participant_id"}
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
    project = _get_header()
    project["_id"] = {"path": "project_id"}
    project["properties"] = _walk_tree(
        project_tree, _munge_properties("project"))
    project['properties']['name'] = project['properties'].pop('project_name')
    project['properties']['code'] = project['properties'].pop('name')
    project['properties'].update(_multfield_template('disease_types'))
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
