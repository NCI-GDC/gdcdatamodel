import argparse
from sqlalchemy import create_engine
import logging

from gdcdatamodel.models import *
from psqlgraph import create_all, Node, Edge


def try_drop_test_data(user, database, root_user='postgres', host=''):

    print('Dropping old test data')

    engine = create_engine("postgres://{user}@{host}/postgres".format(
        user=root_user, host=host))

    conn = engine.connect()
    conn.execute("commit")

    try:
        create_stmt = 'DROP DATABASE "{database}"'.format(database=database)
        conn.execute(create_stmt)
    except Exception as msg:
        logging.warn("Unable to drop test data:" + str(msg))

    conn.close()


def setup_database(user, password, database, root_user='postgres',
                   host='', no_drop=False, no_user=False):
    """
    setup the user and database
    """
    print('Setting up test database')

    if not no_drop:
        try_drop_test_data(user, database)

    engine = create_engine("postgres://{user}@{host}/postgres".format(
        user=root_user, host=host))
    conn = engine.connect()
    conn.execute("commit")

    create_stmt = 'CREATE DATABASE "{database}"'.format(database=database)
    try:
        conn.execute(create_stmt)
    except Exception as msg:
        logging.warn('Unable to create database: {}'.format(msg))

    if not no_user:
        try:
            user_stmt = "CREATE USER {user} WITH PASSWORD '{password}'".format(
                user=user, password=password)
            conn.execute(user_stmt)

            perm_stmt = 'GRANT ALL PRIVILEGES ON DATABASE {database} to {password}'\
                        ''.format(database=database, password=password)
            conn.execute(perm_stmt)
            conn.execute("commit")
        except Exception as msg:
            logging.warn("Unable to add user:" + str(msg))
    conn.close()


def create_tables(host, user, password, database):
    """
    create a table
    """
    print('Creating tables in test database')

    engine = create_engine("postgres://{user}:{pwd}@{host}/{db}".format(
        user=user, host=host, pwd=password, db=database))
    create_all(engine)
    versioned_nodes.Base.metadata.create_all(engine)
    submission.Base.metadata.create_all(engine)

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
    parser.add_argument("--no-drop", action="store_true",
                        default=False, help="do not drop any data")
    parser.add_argument("--no-user", action="store_true",
                        default=False, help="do not create user")

    args = parser.parse_args()

    assert args.host == 'localhost', (
        "Refusing to run on a host that is not localhost! "
        "(This script deletes all the data in the database!)")

    setup_database(args.user, args.password, args.database,
                   no_drop=args.no_drop, no_user=args.no_user)
    create_tables(args.host, args.user, args.password, args.database)
