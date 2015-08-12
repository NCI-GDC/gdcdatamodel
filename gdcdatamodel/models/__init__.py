from psqlgraph import Node, Edge, pg_property
from utils import validate
from misc import *
from sqlalchemy.orm import configure_mappers
from gdcdictionary import GDCDictionary


dictionary = GDCDictionary()
loaded_nodes = [c.__name__ for c in Node.get_subclasses()]
loaded_edges = [c.__name__ for c in Edge.get_subclasses()]


def NodeFactory(name, schema):
    cls = type(name, (Node,), {})
    links = [l['name'] for l in (schema.get('links') or [])]

    for key in schema.get('properties', []):

        if key in links:
            continue

        def setter(self, value):
            self._set_property(key, value)

        setter.__name__ = key
        setattr(cls, key, pg_property(setter))

    return cls


def EdgeFactory(name, label, src_class, dst_class, src_dst_assoc,
                dst_src_assoc):
    cls = type(name, (Edge,), {
        '__label__': label,
        '__table_name': '{}_{}_{}'.format(src_class, label, dst_class),
        '__src_class__': src_class,
        '__dst_class__': dst_class,
        '__src_dst_assoc__': src_dst_assoc,
        '__dst_src_assoc__': dst_src_assoc,
    })
    return cls


def to_mixed_case(val):
    return ''.join([x.title() for x in val.split('_')])


def register_class(cls):
    globals()[cls.__name__] = cls


def load_nodes():
    for entity, subschema in dictionary.schema.iteritems():
        name = subschema['title']
        if name not in loaded_nodes:
            register_class(NodeFactory(name, subschema))


def load_edges():
    for src_label, subschema in dictionary.schema.iteritems():
        for link in subschema.get('links') or []:
            edge_label = link['name']
            dst_label = link['link_to_type']
            src_title = subschema['title']
            dst_title = dictionary.schema[dst_label]['title']
            edge_name = ''.join(map(to_mixed_case, [
                src_label, edge_label, dst_label]))

            register_class(EdgeFactory(
                edge_name, edge_label, src_title, dst_title,
                dst_label+'s', src_label+'s'))

load_nodes()
load_edges()
configure_mappers()
