#!/usr/bin/env python

from psqlgraph import Edge, Node

from gdcdatamodel import models as md

CACHE_EDGES = {
    Node.get_subclass_named(edge.__src_class__): edge
    for edge in Edge.get_subclasses()
    if 'RelatesToCase' in edge.__name__
}


def set_null_edge_columns(graph):
    """Sets null acl, _props, _sysan to empty values for cache edges
    :param graph: PsqlGraphDriver
    """

    session = graph.current_session()

    for edge in CACHE_EDGES.values():
        table = edge.__tablename__
        print(edge)

        acl = "UPDATE {table} SET acl = '{{}}'::text[] where acl is null"
        props = "UPDATE {table} SET _props = '{{}}'::jsonb where _props is null"
        sysan = "UPDATE {table} SET _sysan = '{{}}'::jsonb where _sysan is null"

        session.execute(acl.format(table=table))
        session.execute(props.format(table=table))
        session.execute(sysan.format(table=table))


def main():
    print("No main() action defined, please manually call "
          "set_null_edge_columns(graph)")


if __name__ == '__main__':
    main()
