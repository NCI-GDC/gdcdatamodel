"""
This is a one-time use script to set up a fresh install of Postgres 9.4
Needs to be run as the postgres user.
"""

import argparse
from sqlalchemy import create_engine
import logging

from sqlalchemy import UniqueConstraint
from psqlgraph import Base, PsqlGraphDriver
from psqlgraph.edge import PsqlEdge, add_edge_constraint


def try_drop_test_data(user, database, root_user='postgres', host=''):

    print('Dropping old test data')

    engine = create_engine("postgres://{user}@{host}/postgres".format(
        user=root_user, host=host))

    conn = engine.connect()
    conn.execute("commit")

    try:
        create_stmt = 'DROP DATABASE "{database}"'.format(database=database)
        conn.execute(create_stmt)
    except Exception, msg:
        logging.warn("Unable to drop test data:" + str(msg))

    try:
        user_stmt = "DROP USER {user}".format(user=user)
        conn.execute(user_stmt)
    except Exception, msg:
        logging.warn("Unable to drop test data:" + str(msg))

    conn.close()


def setup_database(user, password, database, root_user='postgres', host=''):
    """
    setup the user and database
    """
    print('Setting up test database')

    try_drop_test_data(user, database)

    engine = create_engine("postgres://{user}@{host}/postgres".format(
        user=root_user, host=host))
    conn = engine.connect()
    conn.execute("commit")

    create_stmt = 'CREATE DATABASE "{database}"'.format(database=database)
    conn.execute(create_stmt)

    try:
        user_stmt = "CREATE USER {user} WITH PASSWORD '{password}'".format(
            user=user, password=password)
        conn.execute(user_stmt)

        perm_stmt = 'GRANT ALL PRIVILEGES ON DATABASE {database} to {user}'\
                    ''.format(database=database, user=user)
        conn.execute(perm_stmt)
        conn.execute("commit")
    except Exception, msg:
        logging.warn("Unable to add user:" + str(msg))
    conn.close()


def create_tables(host, user, password, database):
    """
    create a table
    """
    print('Creating tables in test database')

    driver = PsqlGraphDriver(host, user, password, database)
    add_edge_constraint(UniqueConstraint(
        PsqlEdge.src_id, PsqlEdge.dst_id, PsqlEdge.label))
    Base.metadata.create_all(driver.engine)


def create_indexes(host, user, password, database):
    """
    create a table
    """
    print('Creating indexes')
    driver = PsqlGraphDriver(host, user, password, database)
    index = lambda t, c: ["CREATE INDEX ON {} ({})".format(t, x) for x in c]
    map(driver.engine.execute, index(
        'nodes', [
            'node_id',
            'label',
            'node_id, label'
        ]))
    map(driver.engine.execute, index(
        'edges', [
            'edge_id',
            'src_id',
            'dst_id',
            'label',
            'dst_id, src_id',
            'dst_id, src_id, label'
        ]))
    map(driver.engine.execute, [
        "CREATE INDEX ON nodes USING gin (system_annotations)",
        "CREATE INDEX ON nodes USING gin (properties)",
        "CREATE INDEX ON nodes USING gin (( properties -> 'file_name'))",
        "CREATE INDEX ON nodes USING gin (system_annotations, properties)",
        "CREATE INDEX ON edges USING gin (system_annotations)",
        "CREATE INDEX ON edges USING gin (properties)",
        "CREATE INDEX ON edges USING gin (system_annotations, properties)",
    ])


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, action="store",
                        default='localhost', help="psql-server host")
    parser.add_argument("--user", type=str, action="store",
                        default='test', help="psql test user")
    parser.add_argument("--password", type=str, action="store",
                        default='test', help="psql test password")
    parser.add_argument("--database", type=str, action="store",
                        default='automated_test', help="psql test database")

    args = parser.parse_args()
    setup_database(args.user, args.password, args.database)
    create_tables(args.host, args.user, args.password, args.database)
    create_indexes(args.host, args.user, args.password, args.database)
