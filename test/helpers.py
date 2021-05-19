import psqlgraph
from psqlgraph import Node, Edge, create_all, ext

from gdcdatamodel import models


def truncate(engine, namespace=None):
    """
    Remove data from existing tables
    """
    abstract_node = psqlgraph.Node
    abstract_edge = psqlgraph.Edge
    if namespace:
        abstract_node = ext.get_abstract_node(namespace)
        abstract_edge = ext.get_abstract_edge(namespace)
    conn = engine.connect()
    for table in abstract_node.get_subclass_table_names():
        if table != abstract_node.__tablename__:
            conn.execute('delete from {}'.format(table))
    for table in abstract_edge.get_subclass_table_names():
        if table != abstract_edge.__tablename__:
            conn.execute('delete from {}'.format(table))

    if not namespace:
        # add ng models only to main graph model
        truncate_ng_tables(conn)
    conn.close()


def create_tables(engine, namespace=None):
    """
    create a table
    """

    base = psqlgraph.base.ORMBase
    if namespace:
        base = ext.get_orm_base(namespace)
    create_all(engine, base)

    if not namespace:
        # add ng models only to main graph
        create_ng_tables(engine)


def create_ng_tables(engine):
    models.versioned_nodes.Base.metadata.create_all(engine)
    models.submission.Base.metadata.create_all(engine)
    models.redaction.Base.metadata.create_all(engine)
    models.qcreport.Base.metadata.create_all(engine)
    models.misc.Base.metadata.create_all(engine)


def truncate_ng_tables(conn):

    # Extend this list as needed
    ng_models_metadata = [
        models.versioned_nodes.Base.metadata,
        models.submission.Base.metadata,
        models.redaction.Base.metadata,
        models.qcreport.Base.metadata,
        models.misc.Base.metadata,
    ]

    for meta in ng_models_metadata:
        for table in meta.tables:
            conn.execute("DELETE FROM  {}".format(table))