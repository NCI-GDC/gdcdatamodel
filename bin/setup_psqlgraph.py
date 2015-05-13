"""
This is a one-time use script to set up a fresh install of Postgres 9.4
Needs to be run as the postgres user.
"""

import argparse
from sqlalchemy import create_engine
import logging


from psqlgraph import create_all
from psqlgraph import PsqlGraphDriver, Node, Edge
from gdcdatamodel import models


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

        perm_stmt = 'GRANT ALL PRIVILEGES ON DATABASE {database} to {password}'\
                    ''.format(database=database, password=password)
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
    create_all(driver.engine)


def create_indexes(host, user, password, database):
    """
    create a table
    """
    print('Creating indexes')
    driver = PsqlGraphDriver(host, user, password, database)
    index = lambda t, c: ["CREATE INDEX ON {} ({})".format(t, x) for x in c]
    for cls in Node.get_subclasses():
        table = cls.__tablename__
        map(driver.engine.execute, index(
            table, ['node_id']))
        map(driver.engine.execute, [
            "CREATE INDEX ON {} USING gin (_sysan)".format(table),
            "CREATE INDEX ON {} USING gin (_props)".format(table),
            "CREATE INDEX ON {} USING gin (_sysan, _props)".format(table),
        ])

    for cls in Edge.get_subclasses():
        table = cls.__tablename__
        map(driver.engine.execute, index(
            table, [
                'src_id',
                'dst_id',
                'dst_id, src_id',
            ]))
        map(driver.engine.execute, [
            "CREATE INDEX ON {} USING gin (_sysan)".format(table),
            "CREATE INDEX ON {} USING gin (_props)".format(table),
            "CREATE INDEX ON {} USING gin (_sysan, _props)".format(table),
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
