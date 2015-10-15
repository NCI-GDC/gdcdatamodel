from datetime import datetime
from sqlalchemy.orm import configure_mappers
from sqlalchemy import event, and_
import re
import hashlib
import jsonschema
from gdcdictionary import gdcdictionary
from misc import *
from psqlgraph import Node, Edge, pg_property
from cdisutils import log

logger = log.get_logger('gdcdatamodel')

from sqlalchemy.ext.hybrid import Comparator, hybrid_property

excluded_props = ['id', 'type', 'alias']
dictionary = gdcdictionary
resolver = jsonschema.RefResolver(
    '_definitions.yaml#', gdcdictionary.definitions)

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
    if '$ref' in schema.keys():
        reference, schema = resolver.resolve(schema['$ref'])

    types = schema.get('type')
    types = [types] if not isinstance(types, list) else types

    python_types = [a for t in types for a in {
        'string': [str],
        'number': [float, int, long],
        'integer': [int, long],
        'float': [float],
        'null': [str],
        'boolean': [bool],
        'array': [list],
        None: [str],
    }[t]]
    enum = schema.get('enum')

    # Create pg_property setter
    @pg_property(*python_types, enum=enum)
    def setter(self, val):
        self._set_property(key, val)

    setter.__name__ = name

    return setter


def NodeFactory(_id, title, schema):
    name = remove_spaces(title)
    label = to_camel_case(_id)
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

    # _pg_links are out_edges, links TO other types
    properties['_pg_links'] = {}
    # _pg_backrefs are in_edges, links FROM other types
    properties['_pg_backrefs'] = {}
    # _pg_edges are all edges, links to AND from other types
    properties['_pg_edges'] = {}

    cls = type(name, (Node,), dict(
        __tablename__='node_{}'.format(name.lower()),
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

    unique_keys = schema.get('uniqueKeys', [])
    cls.__pg_secondary_keys = [
        keys for keys in unique_keys if 'id' not in keys
    ]

    class SecondaryKeyComparator(Comparator):
        def __eq__(self, other):
            filters = []
            cls = self.__clause_element__()
            secondary_keys = getattr(cls, '__pg_secondary_keys', [])
            for keys, values in zip(secondary_keys, other):
                if 'id' in keys:
                    continue
                other = {
                    key: val
                    for key, val
                    in zip(keys, values)
                }
                filters.append(cls._props.contains(other))
            return and_(*filters)

    @property
    def _secondary_keys_dicts(self):
        vals = []
        secondary_keys = getattr(self, '__pg_secondary_keys', [])
        for keys in secondary_keys:
            if 'id' in keys:
                continue
            vals.append({key: getattr(self, key, None) for key in keys})
        return vals

    @hybrid_property
    def _secondary_keys(self):
        vals = []
        for keys in getattr(self, '__pg_secondary_keys', []):
            vals.append(tuple(getattr(self, key) for key in keys))
        return tuple(vals)

    @_secondary_keys.comparator
    def _secondary_keys(cls):
        return SecondaryKeyComparator(cls)

    # Set this attribute so psqlgraph doesn't treat it as a property
    _secondary_keys._is_pg_property = False
    cls._secondary_keys = _secondary_keys
    cls._secondary_keys_dicts = _secondary_keys_dicts

    return cls


def EdgeFactory(name, label, src_title, dst_title, src_label,
                dst_label, src_dst_assoc, dst_src_assoc):
    (name, label, src_title, dst_title, src_label, dst_label,
     src_dst_assoc, dst_src_assoc) = map(remove_spaces, [
         name, label, src_title, dst_title, src_label, dst_label,
         src_dst_assoc, dst_src_assoc
     ])

    tablename = 'edge_{}{}{}'.format(
        src_label.replace('_', ''),
        label.replace('_', ''),
        dst_label.replace('_', ''))

    # If the name is too long, prepend it with the first 8 hex of it's hash
    # truncate the name
    if len(tablename) > 40:
        oldname = tablename
        logger.debug('Edge tablename {} too long, shortening'.format(oldname))
        tablename = 'edge_{}_{}'.format(
            str(hashlib.md5(tablename).hexdigest())[:8],
            "{}{}{}".format(
                ''.join([a[:2] for a in src_label.split('_')])[:10],
                ''.join([a[:2] for a in label.split('_')])[:7],
                ''.join([a[:2] for a in dst_label.split('_')])[:10],
            )
        )
        logger.debug('Shortening {} -> {}'.format(oldname, tablename))

    cls = type(name, (Edge,), {
        '__label__': label,
        '__tablename__': tablename,
        '__src_class__': src_title,
        '__dst_class__': dst_title,
        '__src_dst_assoc__': src_dst_assoc,
        '__dst_src_assoc__': dst_src_assoc,
        '__src_table__': Node.get_subclass(src_label).__tablename__,
        '__dst_table__': Node.get_subclass(dst_label).__tablename__,
    })
    return cls


def load_nodes():
    for entity, subschema in dictionary.schema.iteritems():
        name = subschema['title']
        _id = subschema['id']
        if name not in loaded_nodes:
            try:
                register_class(NodeFactory(_id, name, subschema))
            except Exception:
                print 'Unable to load {}'.format(name)
                raise


def parse_edge(src_label, name, edge_label, subschema, link):
    dst_label = link['target_type']
    backref = link['backref']

    src_label = subschema['id']
    src_title = subschema['title']
    if dst_label not in dictionary.schema:
        raise RuntimeError(
            "Destination '{}' for edge '{}' from '{}' not defined"
            .format(dst_label, name, src_label))

    dst_label = dictionary.schema[dst_label]['id']
    dst_title = dictionary.schema[dst_label]['title']

    edge_name = ''.join(map(to_mixed_case, [
        src_label, edge_label, dst_label]))

    src_dst_assoc, dst_src_assoc = special_links.get(
        (src_label, edge_label, dst_label),
        (dst_label+'s', src_label+'s'))

    register_class(EdgeFactory(edge_name, edge_label, src_title,
                               dst_title, src_label, dst_label, name,
                               backref))

    return '_{}_out'.format(edge_name)


def load_edges():
    for src_label, subschema in dictionary.schema.iteritems():

        src_cls = Node.get_subclass(src_label)
        if not src_cls:
            raise RuntimeError('No class labeled {}'.format(src_label))
        for name, link in get_links(subschema).iteritems():
            edge_label = link['label']
            edge_name = parse_edge(
                src_label, name, edge_label, subschema, link)
            src_cls._pg_links[link['name']] = {
                'edge_out': edge_name,
                'dst_type': Node.get_subclass(link['target_type'])
            }


def load_pg_backrefs():
    for src_label, subschema in dictionary.schema.iteritems():
        for name, link in get_links(subschema).iteritems():
            cls = Node.get_subclass(link['target_type'])
            cls._pg_backrefs[link['backref']] = {
                'name': link['name'],
                'src_type': Node.get_subclass(src_label)
            }


def load_pg_edges():
    for cls in Node.get_subclasses():
        for name, link in cls._pg_links.iteritems():
            backref = None
            for prop, br in link['dst_type']._pg_backrefs.iteritems():
                if br['src_type'] == cls:
                    backref = prop
            cls._pg_edges[name] = {
                'backref': backref,
                'type': link['dst_type'],
            }
        for name, backref in cls._pg_backrefs.iteritems():
            cls._pg_edges[name] = {
                'backref': backref['name'],
                'type': backref['src_type'],
            }


load_nodes()
load_edges()
load_pg_backrefs()
load_pg_edges()
configure_mappers()
