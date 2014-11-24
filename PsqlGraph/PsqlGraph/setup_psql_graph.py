import time
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, Boolean, Text
from sqlalchemy.dialects.postgres import *

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

def create_node_table(host, user, password, database):
    """
    create a table 
    """
    
    table_name = 'nodes'

    conn_str = 'postgresql://{user}:{password}@{host}/{database}'.format(
        user=user, password=password, host=host, database=database)

    engine = create_engine(conn_str)
    metadata = MetaData()
    
    """Create the table"""

    new_table = Table(
        table_name, metadata,
        Column('node_id', Text, nullable=False),
        Column('key', Integer, primary_key=True),
        Column('voided', TIMESTAMP),
        Column('created', TIMESTAMP, nullable=False, default=time.time()),
        Column('acl', ARRAY(Text)),
        Column('system_annotations', JSONB, default={}),
        Column('label', Text),
        Column('properties', JSONB, default={}),
    )

    metadata.create_all(engine)

    """Revoke update privileges"""

    immutables = ['node_id', 'key', 'created', 'acl', 'system_annotations', 'label', 'properties']
    conn = engine.connect()
    conn.execute("commit")
    create_stmt = 'REVOKE UPDATE ({immutes}) ON {table} from {user}'.format(
        immutes=','.join(immutables), table=table_name, user=user)
    conn.execute(create_stmt)

def create_edge_table(host, user, password, database):
    """
    create a table 
    """
    
    table_name = 'edges'
    conn_str = 'postgresql://{user}:{password}@{host}/{database}'.format(
        user=user, password=password, host=host, database=database)

    engine = create_engine(conn_str)
    metadata = MetaData()

    new_table = Table(
        table_name, metadata,
        Column('edge_id', Text, nullable=False),
        Column('key', Integer, primary_key=True),
        Column('voided', TIMESTAMP, nullable=False),
        Column('created', TIMESTAMP, nullable=False, default=time.time()),
        Column('src_node', UUID, nullable=False),
        Column('dst_node', UUID, nullable=False),
        Column('system_annotations', JSONB, nullable=False, default={}),
        Column('label', Text),
        Column('properties', JSONB, nullable=False),
    )

    metadata.create_all(engine)

    immutables = ['edge_id', 'key', 'created', 'src_node', 'dst_node', 'system_annotations', 'label', 'properties']
    conn = engine.connect()
    conn.execute("commit")
    create_stmt = 'REVOKE UPDATE ({immutes}) ON {table} from {user}'.format(
        immutes=','.join(immutables), table=table_name, user=user)
    conn.execute(create_stmt)

# if __name__ == '__main__':

    
