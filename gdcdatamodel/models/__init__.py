from sqlalchemy.orm import configure_mappers
from sqlalchemy import select

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
        key: pg_property(
            lambda self, val, key=key: self._set_property(key, val))
        for key, val in schema.get('properties', {}).iteritems()
        if key not in links
        and key not in excluded_props
    }

    if 'alias' in schema.get('properties', {}):

        def setter(self, val):
            self._set_property('submitter_id', val)

        properties['submitter_id'] = pg_property(setter)

    properties['_pg_links'] = {name: Node.get_subclass(l['target_type'])
                               for name, l in links.iteritems()}

    properties['_ownership_traversals'] = schema.get('ownershipTraversals', [])

    cls = type(name, (Node,), dict(
        __label__=label,
        id=node_id,
        **properties
    ))

    def _traverse(self, session, path):
        if not session:
            raise RuntimeError(
                '{} not bound to a session. Cannot lookup project.'
                .format(self))

        q = session.query(Node.get_subclass('project'))
        for e in path:
            q = q.join(*getattr(q.entity(), e).attr)
        return q.ids(self.node_id)

    @property
    def _projects(self):
        session = self.get_session()
        queries = map(lambda p: self._traverse(session, p),
                      self._ownership_traversals)
        if not queries:
            return []
        query = queries[0]
        for q in queries[1:]:
            query = query.union(q)
        return query.all()

    @property
    def _project_ids(self):
        return ['{}-{}'.format(p.programs[0].name, p.code)
                for p in self._projects]

    def _is_in_project(self, project_id):
        return any([project_id == p for p in self._project_ids])

    cls._projects = _projects
    cls._traverse = _traverse
    cls._project_ids = _project_ids
    cls._is_in_project = _is_in_project

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
