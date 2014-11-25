import time

from PsqlGraph import Base, PsqlGraphDriver
from sqlalchemy import create_engine, select, MetaData, Table, Column, Integer, Text

from cdisutils import log
logger = log.get_logger(__name__)

"""
This is a one-time use script to set up a fresh install of Postgres 9.4
Needs to be run as the postgres user.
"""

def try_drop_test_data(user, database):

    engine = create_engine("postgres://postgres@/postgres")
    conn = engine.connect()
    conn.execute("commit")

    try:
        create_stmt = 'DROP DATABASE "{database}"'.format(database=database)
        conn.execute(create_stmt)

        user_stmt = "DROP USER {user}".format(user=user)
        conn.execute(user_stmt)

    except Exception, msg:
        logger.warn("Unable to drop test data:" + str(msg))

    else:
        conn.close()


def setup_database(user, password, database):
    """
    setup the user and database
    """

    try_drop_test_data(user, database)

    engine = create_engine("postgres://postgres@/postgres")
    conn = engine.connect()
    conn.execute("commit")

    create_stmt = 'CREATE DATABASE "{database}"'.format(database=database)
    conn.execute(create_stmt)
    
    user_stmt = "CREATE USER {user} WITH PASSWORD '{password}'".format(user=user, password=password)
    conn.execute(user_stmt)

    perm_stmt = 'GRANT ALL PRIVILEGES ON DATABASE {database} to {password}'.format(
        database=database, password=password)
    
    conn.close()

def create_tables(host, user, password, database):
    """
    create a table 
    """
    
    driver = PsqlGraphDriver(host, user, password, database)
    Base.metadata.create_all(driver.engine)
    
    
