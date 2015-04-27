import models as m
from mapped_entities import (
    file_tree, file_traversal,
    participant_tree, participant_traversal,
    annotation_tree, annotation_traversal,
    project_tree, annotation_tree,
    ONE_TO_MANY, ONE_TO_ONE
)
from addict import Dict
from collections import defaultdict
from psqlgraph import Node, Edge

STRING = {'index': 'not_analyzed', 'type': 'string'}
LONG = {'type': 'long'}
INTEGER = {'type': 'int'}
FLATTEN = ['tag', 'platform', 'data_format', 'experimental_strategy']

MULTIFIELDS = {
    'project': ['code', 'disease_type', 'name', 'primary_site'],
    'annotation': ['annotation_id', 'entity_id'],
    'files': ['file_id', 'file_name'],
    'participant': ['participant_id', 'submitter_id'],
}


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


def _get_header(source):
    header = Dict()
    header.dynamic = 'strict'
    header._all.enabled = False
    header._source.compress = True
    header._source.excludes = ["__comment__"]
    header._id = {'path': '{}_id'.format(source)}
    return header


def _get_es_type(_type):
    if long in _type or int in _type:
        return 'long'
    else:
        return 'string'


def _munge_properties(source, nested=True):

    # Get properties from schema
    cls = Node.get_subclass(source)
    properties = cls.get_pg_properties()
    fields = properties.keys()

    # Add id to document
    id_name = '{}_id'.format(source)
    doc = Dict({id_name: STRING})

    # Add all properties to document
    for field in fields:
        _type = _get_es_type(properties[field] or [])
        # assign the type
        doc[field] = {'type': _type}
        if str(_type) == 'string':
            doc[field]['index'] = 'not_analyzed'
    return doc


def multifield(name):
    doc = Dict()
    doc.type = 'string'

    # Raw
    doc.fields.raw.index = 'not_analyzed'
    doc.fields.raw.store = 'yes'
    doc.fields.raw.type = 'string'

    # Analyzed
    doc.fields.analyzed.index = "analyzed"
    doc.fields.analyzed.index_analyzer = "id_index"
    doc.fields.analyzed.search_analyzer = "id_search"
    doc.fields.analyzed.type = "string"

    # Search
    doc.fields.search.index = 'analyzed'
    doc.fields.search.analyzer = 'id_search'
    doc.fields.search.type = 'string'
    return Dict({name: doc})


def _walk_tree(tree, mapping):
    for k, v in [(k, v) for k, v in tree.items() if k != 'corr']:
        corr, name = v['corr']
        if name not in mapping:
            mapping[name] = {'properties': {}}
        if k in FLATTEN:
            mapping[name] = STRING
        elif k == 'annotation':
            mapping.annotations = annotation_body()
            mapping.annotations.type = 'nested'
        else:
            nested = (corr == ONE_TO_MANY)
            mapping[name].properties.update(
                _munge_properties(k, nested))
            _walk_tree(tree[k], mapping[name]['properties'])
            if nested:
                mapping[name]['type'] = 'nested'
    return mapping


def flatten_data_type(root):
    root.data_subtype = STRING
    root.data_type = STRING


def patch_file_timestamps(doc):
    doc.properties.uploaded_datetime = LONG
    doc.properties.published_datetime = LONG


def nested(source):
    return Dict(type='nested', properties=_munge_properties(source))


def add_multifields(doc, source):
    for key in MULTIFIELDS[source]:
        doc.properties.update(multifield(key))


def patch_project(doc):
    doc.pop('code')


def get_file_es_mapping(include_participant=True):
    files = _get_header('file')
    files.properties = _walk_tree(file_tree, _munge_properties('file'))
    flatten_data_type(files.properties)

    # Specify the entity the file was derived from
    files.properties.associated_entities.type = 'nested'
    files.properties.associated_entities.properties.entity_type = STRING
    files.properties.associated_entities.properties.entity_id = STRING
    files.properties.associated_entities.properties.participant_id = STRING

    # Patch file mutlifields
    add_multifields(files, 'files')

    # Related files
    related_files = nested('file')
    related_files.properties.type = STRING
    related_files.properties.data_type = STRING
    related_files.properties.data_subtype = STRING
    patch_file_timestamps(related_files)
    files.properties.related_files = related_files

    # Temporary until datetimes are backported
    patch_file_timestamps(files)

    # File access
    files.properties.access = STRING
    files.properties.acl = STRING

    # Other file properties
    files.properties.origin = STRING

    # Participant
    files.properties.pop('participant', None)
    if include_participant:
        files.properties.participants = get_participant_es_mapping(False)
        files.properties.participants.type = 'nested'
    return files.to_dict()


def get_participant_es_mapping(include_file=True):
    # participant body
    participant = _get_header('participant')
    participant.properties = _walk_tree(
        participant_tree, _munge_properties('participant'))
    participant.properties.days_to_index = LONG

    # Patch project
    patch_project(participant.properties.project.properties)

    # Patch participant mutlifields
    add_multifields(participant, 'participant')

    # Metadata files
    participant.properties.metadata_files = nested('file')
    participant.properties.metadata_files.properties.data_type = STRING
    participant.properties.metadata_files.properties.data_subtype = STRING
    participant.properties.metadata_files.properties.acl = STRING

    # Add pop whatever file is present and add correct files
    participant.properties.pop('file', None)
    if include_file:
        participant.properties.files = get_file_es_mapping(True)
        participant.properties.files.type = 'nested'

    # Adjust file properties
    participant.properties.files.properties.pop('associated_entities', None)
    participant.properties.files.properties.pop('annotations', None)

    # Summary
    summary = participant.properties.summary.properties
    summary.file_count = LONG
    summary.file_size = LONG

    # Summary experimental strategies
    summary.experimental_strategies.type = 'nested'
    summary.experimental_strategies.properties.experimental_strategy = STRING
    summary.experimental_strategies.properties.file_count = LONG

    # Summary data types
    summary.data_types.type = 'nested'
    summary.data_types.properties.data_type = STRING
    summary.data_types.properties.file_count = LONG

    # Clinical
    clinical = participant.properties.clinical.properties
    clinical.age_at_diagnosis = INTEGER
    clinical.days_to_death = INTEGER

    return participant.to_dict()


def annotation_body(nested=True):
    annotation = Dict()
    annotation.properties = _munge_properties('annotation', nested)
    annotation.properties.participant_id = STRING
    annotation.properties.entity_type = STRING
    annotation.properties.entity_id = STRING
    annotation.properties.entity_submitter_id = STRING
    annotation.properties.pop('item_id', None)
    return annotation


def get_annotation_es_mapping(include_file=True):
    annotation = _get_header('annotation')
    annotation.update(annotation_body(nested=False))

    # Patch annotation mutlifields
    add_multifields(annotation, 'annotation')

    # Add the project and program
    annotation.properties.update(Dict({
        'project': {'properties': _munge_properties('project')}}))
    annotation.properties.project.properties.program = {
        'properties': _munge_properties('program')}

    return annotation.to_dict()


def get_project_es_mapping():
    project = _get_header('project')
    project.properties = _walk_tree(project_tree, _munge_properties('project'))

    # Patch annotation mutlifields
    add_multifields(project, 'project')

    # Patch project
    patch_project(project.properties)
    project.properties.update(multifield('project_id'))

    # Summary
    summary = project.properties.summary.properties
    summary.file_count = LONG
    summary.file_size = LONG
    summary.participant_count = LONG

    # Summary experimental strategies
    summary.experimental_strategies.type = 'nested'
    summary.experimental_strategies.properties.participant_count = LONG
    summary.experimental_strategies.properties.experimental_strategy = STRING
    summary.experimental_strategies.properties.file_count = LONG

    # Summary data types
    summary.data_types.type = 'nested'
    summary.data_types.properties.participant_count = LONG
    summary.data_types.properties.data_type = STRING
    summary.data_types.properties.file_count = LONG

    return project.to_dict()
