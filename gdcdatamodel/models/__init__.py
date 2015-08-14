from sqlalchemy.orm import configure_mappers
import re

from gdcdictionary import GDCDictionary
from misc import *
from psqlgraph import Node, Edge, pg_property
from utils import validate

excluded_props = ['id', 'type', 'alias']
dictionary = GDCDictionary()

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


def NodeFactory(title, schema):
    name = remove_spaces(title)
    label = to_camel_case(name)

    links = [l['name'] for l in (schema.get('links') or [])]

    @property
    def node_id(self, value):
        return self.node_id

    @node_id.setter
    def node_id(self, value):
        self.node_id = value

    properties = {
        key: pg_property(
            lambda self, val, key=key: self._set_property(key, val))
        for key, val in schema.get('properties', {}).iteritems()
        if key not in links
        and key not in excluded_props
    }

    if 'alias' in schema.get('properties', {}):
        properties['submitter_id'] = pg_property(
            lambda self, val: self._set_property('submitter_id', val))

    cls = type(name, (Node,), dict(
        __label__=label,
        id=node_id,
        **properties
    ))

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


def parse_edge(src_label, edge_label, subschema, link):
    dst_label = link['link_to_type']
    src_title = subschema['title']
    dst_title = dictionary.schema[dst_label]['title']
    edge_name = ''.join(map(to_mixed_case, [
        src_label, edge_label, dst_label]))

    src_dst_assoc, dst_src_assoc = special_links.get(
        (src_label, edge_label, dst_label),
        (dst_label+'s', src_label+'s'))

    register_class(EdgeFactory(
        edge_name, edge_label, src_title, dst_title,
        src_dst_assoc, dst_src_assoc))


def load_edges():
    for src_label, subschema in dictionary.schema.iteritems():
        for links in subschema.get('links') or []:
            if 'anyOf' in links:
                [parse_edge(src_label, links['name'], subschema, l)
                 for l in links['anyOf']]
            if 'oneOf' in links:
                [parse_edge(src_label, links['name'], subschema, l)
                 for l in links['oneOf']]
            if 'anyOf' not in links and 'oneOf' not in links:
                parse_edge(src_label, links['name'], subschema, links)

load_nodes()
load_edges()
configure_mappers()
