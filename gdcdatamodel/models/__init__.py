from datetime import datetime
from sqlalchemy.orm import configure_mappers
from sqlalchemy import event
import re

from gdcdictionary import gdcdictionary
from misc import *
from psqlgraph import Node, Edge, pg_property
from utils import validate

excluded_props = ['id', 'type', 'alias']
dictionary = gdcdictionary

loaded_nodes = [c.__name__ for c in Node.get_subclasses()]
loaded_edges = [c.__name__ for c in Edge.get_subclasses()]


special_links = {
    ('file', 'related_to', 'file'): (
        'related_files', 'parent_files'),
    ('file', 'data_from', 'file'): (
        'derived_files', 'source_files'),
    ('file', 'describes', 'case'): (
        'described_cases', 'describing_files'),
    ('archive', 'related_to', 'file'): (
        'related_to_files', 'related_archives'),
    ('file', 'member_of', 'experimental_strategy'): (
        'experimental_strategies', 'files'),
}


def to_camel_case(val):
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', val).lower()


def to_mixed_case(val):
    return ''.join([x.title() for x in val.split('_')])


def remove_spaces(self):
    return self.replace(' ', '')


def register_class(cls):
    globals()[cls.__name__] = cls


def get_links(schema):
    links = {}
    for entry in schema.get('links') or []:
        if 'subgroup' in entry:
            for link in entry['subgroup']:
                links[link['name']] = link
        else:
            links[entry['name']] = entry
    return links


def PropertyFactory(name, schema, key=None):
    key = name if key is None else key

    # Lookup and translate types
    types = schema.get('type')
    types = [types] if not isinstance(types, list) else types
    python_types = [a for t in types for a in {
        'string': [str],
        'date-time': [str],
        'number': [float, int, long],
        'integer': [int, long],
        'float': [float],
        'null': [str],
        'boolean': [bool],
        None: [str],
    }[t]]
    enum = schema.get('enum')

    # Create pg_property setter
    @pg_property(*python_types, enum=enum)
    def setter(self, val):
        self._set_property(key, val)

    setter.__name__ = name

    return setter


def NodeFactory(title, schema):
    name = remove_spaces(title)
    label = to_camel_case(name)
    links = get_links(schema)

    @property
    def node_id(self, value):
        return self.node_id

    @node_id.setter
    def node_id(self, value):
        self.node_id = value

    properties = {
        key: PropertyFactory(key, schema)
        for key, schema in schema.get('properties', {}).iteritems()
        if key not in links
        and key not in excluded_props
    }

    if 'alias' in schema.get('properties', {}):
        properties['submitter_id'] = PropertyFactory(
            'alias', schema['properties']['alias'], 'submitter_id')

    properties['_pg_links'] = {name: Node.get_subclass(l['target_type'])
                               for name, l in links.iteritems()}

    cls = type(name, (Node,), dict(
        __label__=label,
        id=node_id,
        **properties
    ))

    @event.listens_for(cls, 'before_insert')
    def set_created_updated_datetimes(mapper, connection, target):
        ts = target.get_session()._flush_timestamp.isoformat('T')
        if 'updated_datetime' in target.props:
            target._props['updated_datetime'] = ts
        if 'created_datetime' in target.props:
            target._props['created_datetime'] = ts

    @event.listens_for(cls, 'before_update')
    def set_updated_datetimes(mapper, connection, target):
        ts = target.get_session()._flush_timestamp.isoformat('T')
        if 'updated_datetime' in target.props:
            target._props['updated_datetime'] = ts

    return cls


def EdgeFactory(name, label, src_class, dst_class, src_dst_assoc,
                dst_src_assoc):
    name, label, src_class, dst_class, src_dst_assoc, dst_src_assoc = map(
        remove_spaces, [name, label, src_class, dst_class,
                        src_dst_assoc, dst_src_assoc])
    cls = type(name, (Edge,), {
        '__label__': label,
        '__table_name': '{}_{}_{}'.format(src_class, label, dst_class),
        '__src_class__': src_class,
        '__dst_class__': dst_class,
        '__src_dst_assoc__': src_dst_assoc,
        '__dst_src_assoc__': dst_src_assoc,
    })
    return cls


def load_nodes():
    for entity, subschema in dictionary.schema.iteritems():
        name = subschema['title']
        if name not in loaded_nodes:
            register_class(NodeFactory(name, subschema))


def parse_edge(src_label, name, edge_label, subschema, link):
    dst_label = link['target_type']
    backref = link['backref']

    src_title = subschema['title']
    dst_title = dictionary.schema[dst_label]['title']
    edge_name = ''.join(map(to_mixed_case, [
        src_label, edge_label, dst_label]))

    src_dst_assoc, dst_src_assoc = special_links.get(
        (src_label, edge_label, dst_label),
        (dst_label+'s', src_label+'s'))

    register_class(EdgeFactory(
        edge_name, edge_label, src_title, dst_title, name, backref))

    return '_{}_out'.format(edge_name)


def load_edges():
    for src_label, subschema in dictionary.schema.iteritems():
        src_cls = Node.get_subclass(src_label)
        src_cls._pg_links = {}
        for name, link in get_links(subschema).iteritems():
            edge_label = link['label']
            edge_name = parse_edge(
                src_label, name, edge_label, subschema, link)
            src_cls._pg_links[link['name']] = {
                'edge_out': edge_name,
                'dst_type': Node.get_subclass(link['target_type'])
            }

load_nodes()
load_edges()
configure_mappers()
