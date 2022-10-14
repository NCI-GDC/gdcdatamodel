"""
gdcdatamodel.gdc_postgres_admin
----------------------------------

Module for stateful management of a GDC PostgreSQL installation.
"""

import argparse
import logging
import random
import time

import sqlalchemy as sa
from psqlgraph import create_all, ext
from psqlgraph.base import ORMBase
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

#: Required but 'unused' import to register GDC models
from . import models  # noqa

logging.basicConfig()
logger = logging.getLogger("gdc_postgres_admin")
logger.setLevel(logging.INFO)

name_root = "table_creator_"
app_name = "{}{}".format(name_root, random.randint(1000, 9999))


GRANT_READ_PRIVS_SQL = """
BEGIN;
GRANT SELECT ON TABLE {table} TO {user};
COMMIT;
"""

GRANT_WRITE_PRIVS_SQL = """
BEGIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table} TO {user};
COMMIT;
"""

REVOKE_READ_PRIVS_SQL = """
BEGIN;
REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLE {table} FROM {user};
COMMIT;
"""

REVOKE_WRITE_PRIVS_SQL = """
BEGIN;
REVOKE INSERT, UPDATE, DELETE ON TABLE {table} FROM {user};
COMMIT;
"""


def execute(engine, sql, *args, **kwargs):
    statement = sa.sql.text(sql)
    logger.debug(statement)
    return engine.execute(statement, *args, **kwargs)


def get_engine(host, user, password, database):
    connect_args = {"application_name": app_name}
    con_str = "postgres://{user}:{pwd}@{host}/{db}".format(
        user=user, host=host, pwd=password, db=database
    )
    return create_engine(con_str, connect_args=connect_args)


def execute_for_all_graph_tables(engine, sql, namespace=None, **kwargs):
    """Execute a SQL statment that has a python format variable {table}
    to be replaced with the tablename for all Node and Edge tables

    """
    node_cls = ext.get_abstract_node(namespace)
    edge_cls = ext.get_abstract_edge(namespace)

    for cls in node_cls.get_subclasses() + edge_cls.get_subclasses():
        _kwargs = dict(kwargs, **{'table': cls.__tablename__})
        statement = sql.format(**_kwargs)
        execute(engine, statement)


def grant_read_permissions_to_graph(engine, user, namespace=None):
    execute_for_all_graph_tables(engine, GRANT_READ_PRIVS_SQL, namespace, user=user)


def grant_write_permissions_to_graph(engine, user, namespace=None):
    execute_for_all_graph_tables(engine, GRANT_WRITE_PRIVS_SQL, namespace, user=user)


def revoke_read_permissions_to_graph(engine, user, namespace=None):
    execute_for_all_graph_tables(engine, REVOKE_READ_PRIVS_SQL, namespace, user=user)


def revoke_write_permissions_to_graph(engine, user, namespace=None):
    execute_for_all_graph_tables(engine, REVOKE_WRITE_PRIVS_SQL, namespace, user=user)


def create_graph_tables(engine, timeout, namespace=None):
    """
    create a table
    """
    logger.info('Creating tables (timeout: %d)', timeout)

    connection = engine.connect()
    trans = connection.begin()
    logger.info("Setting lock_timeout to %d", timeout)

    timeout_str = '{}s'.format(int(timeout+1))
    connection.execute("SET LOCAL lock_timeout = %s;", timeout_str)

    orm_base = ext.get_orm_base(namespace) if namespace else ORMBase
    create_all(connection, base=orm_base)
    trans.commit()


def create_tables(engine, delay, retries, namespace=None):
    """Create the tables but do not kill any blocking processes.

    This command will catch OperationalErrors signalling timeouts from
    the database when the lock was not obtained successfully within
    the `delay` period.

    """

    logger.info('Running table creator named %s', app_name)
    try:
        return create_graph_tables(engine, delay, namespace=namespace)

    except OperationalError as e:
        if 'timeout' in str(e):
            logger.warning('Attempt timed out')
        else:
            raise

        if retries <= 0:
            raise RuntimeError('Max retries exceeded')

        logger.info(
            'Trying again in {} seconds ({} retries remaining)'
            .format(delay, retries))
        time.sleep(delay)

        create_tables(engine, delay, retries-1, namespace=namespace)


def subcommand_create(args):
    """Idempotently/safely create ALL tables in database that are required
    for the GDC.  This command will not delete/drop any data.

    """

    logger.info("Running subcommand 'create'")
    engine = get_engine(args.host, args.user, args.password, args.database)
    kwargs = dict(
        engine=engine,
        delay=args.delay,
        retries=args.retries,
        namespace=args.namespace
    )

    return create_tables(**kwargs)


def subcommand_grant(args):
    """Grant permissions to a user.

    Argument ``--read`` will grant users read permissions
    Argument ``--write`` will grant users write and READ permissions
    """

    logger.info("Running subcommand 'grant'")
    engine = get_engine(args.host, args.user, args.password, args.database)

    assert args.read or args.write, 'No premission types/users specified.'

    if args.read:
        users_read = [u for u in args.read.split(',') if u]
        for user in users_read:
            grant_read_permissions_to_graph(engine, user, args.namespace)

    if args.write:
        users_write = [u for u in args.write.split(',') if u]
        for user in users_write:
            grant_write_permissions_to_graph(engine, user, args.namespace)


def subcommand_revoke(args):
    """Grant permissions to a user.

    Argument ``--read`` will revoke users' read permissions
    Argument ``--write`` will revoke users' write AND READ permissions
    """

    logger.info("Running subcommand 'revoke'")
    engine = get_engine(args.host, args.user, args.password, args.database)

    if args.read:
        users_read = [u for u in args.read.split(',') if u]
        for user in users_read:
            revoke_read_permissions_to_graph(engine, user, args.namespace)

    if args.write:
        users_write = [u for u in args.write.split(',') if u]
        for user in users_write:
            revoke_write_permissions_to_graph(engine, user, args.namespace)


def add_base_args(subparser):
    subparser.add_argument("-H", "--host", type=str, action="store",
                           required=True, help="psql-server host")
    subparser.add_argument("-U", "--user", type=str, action="store",
                           required=True, help="psql test user")
    subparser.add_argument("-D", "--database", type=str, action="store",
                           required=True, help="psql test database")
    subparser.add_argument("-P", "--password", type=str, action="store",
                           default='', help="psql test password")
    subparser.add_argument("-N", "--namespace", type=lambda x: x if x else None,
                           help="psqlgraph model namespace")
    return subparser


def add_subcommand_create(subparsers):
    parser = add_base_args(subparsers.add_parser(
        'graph-create',
        help=subcommand_create.__doc__
    ))
    parser.add_argument(
        "--delay", type=int, action="store", default=60,
        help="How many seconds to wait for blocking processes to finish before retrying."
    )
    parser.add_argument(
        "--retries", type=int, action="store", default=10,
        help="If blocked by important process, how many times to retry after waiting `delay` seconds."
    )


def add_subcommand_grant(subparsers):
    parser = add_base_args(subparsers.add_parser(
        'graph-grant',
        help=subcommand_grant.__doc__
    ))
    parser.add_argument(
        "--read", type=str, action="store",
        help="Users to grant read access to (comma separated)."
    )
    parser.add_argument(
        "--write", type=str, action="store",
        help="Users to grant read/write access to (comma separated)."
    )


def add_subcommand_revoke(subparsers):
    parser = add_base_args(subparsers.add_parser(
        'graph-revoke',
        help=subcommand_revoke.__doc__
    ))
    parser.add_argument(
        "--read", type=str, action="store",
        help="Users to revoke read access from (comma separated)."
    )
    parser.add_argument(
        "--write", type=str, action="store",
        help=("Users to revoke write access from (comma separated). "
              "NOTE: The user will still have read privs!!")
    )


def get_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcommand")
    add_subcommand_create(subparsers)
    add_subcommand_grant(subparsers)
    add_subcommand_revoke(subparsers)
    return parser


def main(args=None):
    args = args or get_parser().parse_args()

    logger.info("[ HOST     : %-10s ]", args.host)
    logger.info("[ DATABASE : %-10s ]", args.database)
    logger.info("[ USER     : %-10s ]", args.user)
    logger.info("[ NAMESPACE     : %-10s ]", args.namespace or "default")

    return_value = {
        'graph-create': subcommand_create,
        'graph-grant': subcommand_grant,
        'graph-revoke': subcommand_revoke,
    }[args.subcommand](args)

    logger.info("Done.")
    return return_value
