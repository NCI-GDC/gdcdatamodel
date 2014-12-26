import argparse
from psqlgraph import Base, PsqlGraphDriver
from sqlalchemy import create_engine
import logging

"""
This is a one-time use script to set up a fresh install of Postgres 9.4
Needs to be run as the postgres user.
"""


def try_drop_test_data(user, database, root_user='postgres', host=''):

    print('Dropping old test data')

    engine = create_engine("postgres://{user}@{host}/postgres".format(
        user=root_user, host=host))

    conn = engine.connect()
    conn.execute("commit")

    try:
        create_stmt = 'DROP DATABASE "{database}"'.format(database=database)
        conn.execute(create_stmt)

        user_stmt = "DROP USER {user}".format(user=user)
        conn.execute(user_stmt)

    except Exception, msg:
        logging.warn("Unable to drop test data:" + str(msg))

    else:
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

    user_stmt = "CREATE USER {user} WITH PASSWORD '{password}'".format(
        user=user, password=password)
    conn.execute(user_stmt)

    perm_stmt = 'GRANT ALL PRIVILEGES ON DATABASE {database} to {password}'\
                ''.format(database=database, password=password)
    conn.execute(perm_stmt)

    conn.execute("commit")

    conn.close()


def create_tables(host, user, password, database):
    """
    create a table
    """
    print('Creating tables in test database')

    driver = PsqlGraphDriver(host, user, password, database)
    Base.metadata.create_all(driver.engine)

    conn = driver.engine.connect()

    null_stmt = """
    CREATE UNIQUE INDEX constraint_voided_null
    ON nodes (node_id)
    WHERE voided IS NULL;
    """
    conn.execute(null_stmt)

    conn.close()

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
