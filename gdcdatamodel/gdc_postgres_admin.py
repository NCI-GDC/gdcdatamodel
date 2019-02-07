# -*- coding: utf-8 -*-
"""
gdcdatamodel.gdc_postgres_admin
----------------------------------

Module for stateful management of a GDC PostgreSQL installation.
"""

import argparse
import logging
import random
import sqlalchemy as sa
import time

from collections import namedtuple
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

#: Required but 'unused' import to register GDC models
from . import models  # noqa

from psqlgraph import (
    create_all,
    Node,
    Edge,
)

logging.basicConfig()
logger = logging.getLogger("gdc_postgres_admin")
logger.setLevel(logging.INFO)

name_root = "table_creator_"
app_name = "{}{}".format(name_root, random.randint(1000, 9999))
no_kill_list = []
BlockingQueryResult = namedtuple('BlockingQueryResult', [
    'blocked_appname',
    'blocked_pid',
    'blocking_appname',
    'blocking_pid',
    'blocking_statement',
])


# See https://wiki.postgresql.org/wiki/Lock_Monitoring
BLOCKING_SQL = """

SELECT
    blocked_activity.application_name  AS blocked_appname,
    blocked_locks.pid                  AS blocked_pid,

    blocking_activity.application_name AS blocking_appname,
    blocking_locks.pid                 AS blocking_pid,

    blocking_activity.query            AS blocking_statement

FROM pg_catalog.pg_locks               blocked_locks

JOIN pg_catalog.pg_stat_activity       blocked_activity
    ON blocked_activity.pid            = blocked_locks.pid

JOIN pg_catalog.pg_locks               blocking_locks
    ON  blocking_locks.locktype        = blocked_locks.locktype
    AND blocking_locks.DATABASE        IS NOT DISTINCT FROM blocked_locks.DATABASE
    AND blocking_locks.relation        IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page            IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple           IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.virtualxid      IS NOT DISTINCT FROM blocked_locks.virtualxid
    AND blocking_locks.transactionid   IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid         IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid           IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid        IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid             != blocked_locks.pid

JOIN pg_catalog.pg_stat_activity blocking_activity
     ON blocking_activity.pid          = blocking_locks.pid

WHERE NOT blocked_locks.GRANTED
      AND blocked_activity.application_name = :app_name;

"""


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


def execute_for_all_graph_tables(engine, sql, *args, **kwargs):
    """Execute a SQL statment that has a python format variable {table}
    to be replaced with the tablename for all Node and Edge tables

    """
    for cls in Node.__subclasses__() + Edge.__subclasses__():
        _kwargs = dict(kwargs, **{'table': cls.__tablename__})
        statement = sql.format(**_kwargs)
        execute(engine, statement)


def grant_read_permissions_to_graph(engine, user):
    execute_for_all_graph_tables(engine, GRANT_READ_PRIVS_SQL, user=user)


def grant_write_permissions_to_graph(engine, user):
    execute_for_all_graph_tables(engine, GRANT_WRITE_PRIVS_SQL, user=user)


def revoke_read_permissions_to_graph(engine, user):
    execute_for_all_graph_tables(engine, REVOKE_READ_PRIVS_SQL, user=user)


def revoke_write_permissions_to_graph(engine, user):
    execute_for_all_graph_tables(engine, REVOKE_WRITE_PRIVS_SQL, user=user)


def migrate_transaction_snapshots(engine, user):
    """
    Updates to TransactionSnapshot table:
        - change old `id` column to `entity_id`, which is no longer unique or primary
          key
        - add new serial `id` column as primary key
    """
    md = MetaData(bind=engine)
    tablename = models.submission.TransactionSnapshot.__tablename__
    snapshots_table = Table(tablename, md, autoload=True)
    if "entity_id" not in snapshots_table.c:
        execute(
            engine,
            "ALTER TABLE {name} DROP CONSTRAINT {name}_pkey".format(name=tablename),
        )
        execute(
            engine,
            "ALTER TABLE {} RENAME id TO entity_id".format(tablename),
        )
        execute(
            engine,
            "ALTER TABLE {} ADD COLUMN id SERIAL PRIMARY KEY;".format(tablename),
        )


def create_graph_tables(engine, timeout):
    """
    create a table
    """
    logger.info('Creating tables (timeout: %d)', timeout)

    connection = engine.connect()
    trans = connection.begin()
    logger.info("Setting lock_timeout to %d", timeout)

    timeout_str = '{}s'.format(int(timeout+1))
    connection.execute("SET LOCAL lock_timeout = %s;", timeout_str)

    create_all(connection)
    trans.commit()


def is_blocked_by_no_kill(blocking):
    for proc in blocking:
        if proc.blocking_appname in no_kill_list:
            print 'Blocked by no-kill process {}, {}: {}'.format(
                proc.blocking_appname, proc.blocking_pid,
                proc.blocking_statement)
            return True
    return False


def lookup_blocking_psql_backend_processes(engine):
    """
    """

    sql_cmd = sa.sql.text(BLOCKING_SQL)
    conn = engine.connect()
    blocking = conn.execute(sql_cmd, app_name=app_name)
    return [BlockingQueryResult(*b) for b in blocking]


def kill_blocking_psql_backend_processes(engine):
    """Query the postgres backend tables for the process that is blocking
    this app, as identified by the `app_name`.

    .. warning:: **THIS COMMAND KILLS OTHER PEOPLES POSTGRES QUERIES.**

    It is sometimes necessary to kill other peoples queries in order
    to gain a write lock on a table to ALTER it for a foreign-key from
    a new table.

    There is a list at the top of this module that specifies which
    processes are 'no-kill'.  There currently are none, but a good
    exmaple of one that you might want to put in there is the
    Elasticsearch build process, since you might not want to kill a 5h
    long process 4h in.

    """

    blockers = lookup_blocking_psql_backend_processes(engine)

    if is_blocked_by_no_kill(blockers):
        logger.warn("Process blocked by a 'no-kill' process. "
                    "Refusing to kill it")
        return

    if not blockers:
        logger.warning("Found %d blocking processes!", len(blockers))
    else:
        logger.info("Found %d blocking processes", len(blockers))

    for result in blockers:
        logger.warning(
            'Killing blocking backend process: name({})\tpid({}): {}'.format(
                result.blocking_appname,
                result.blocking_pid,
                result.blocking_statement)
        )

        # Kill anything in the way, it was deemed of low importance
        sql_cmd = 'SELECT pg_terminate_backend({blocking_pid});'.format(
            blocking_pid=result.blocking_pid
        )
        execute(engine, sql_cmd)


def create_tables_force(engine, delay, retries):
    """Create the tables and **KILL ANY BLOCKING PROCESSES**.

    This command will spawn a process to create the new tables in
    order to find out which process is blocking us.  If we didn't do
    this concurrently, then the table creation will have disappeared
    by the time we tried to find its blocker in the postgres backend
    tables.

    """

    logger.info('Running table creator named %s', app_name)
    logger.warning('Running with force=True option %s', app_name)

    from multiprocessing import Process
    p = Process(target=create_graph_tables, args=(engine, delay))
    p.start()
    time.sleep(delay)

    if p.is_alive():
        logger.warning('Table creation blocked!')
        kill_blocking_psql_backend_processes(engine)

        #  Wait some time for table creation to proceed
        time.sleep(4)

    if p.is_alive():
        if retries <= 0:
            raise RuntimeError('Max retries exceeded.')

        logger.warning('Table creation failed, retrying.')
        return create_tables_force(engine, delay, retries-1)


def create_tables(engine, delay, retries):
    """Create the tables but do not kill any blocking processes.

    This command will catch OperationalErrors signalling timeouts from
    the database when the lock was not obtained successfully within
    the `delay` period.

    """

    logger.info('Running table creator named %s', app_name)
    try:
        return create_graph_tables(engine, delay)

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

        create_tables(engine, delay, retries-1)


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
    )

    if args.force:
        return create_tables_force(**kwargs)
    else:
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
            grant_read_permissions_to_graph(engine, user)

    if args.write:
        users_write = [u for u in args.write.split(',') if u]
        for user in users_write:
            grant_write_permissions_to_graph(engine, user)


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
            revoke_read_permissions_to_graph(engine, user)

    if args.write:
        users_write = [u for u in args.write.split(',') if u]
        for user in users_write:
            revoke_write_permissions_to_graph(engine, user)


def add_base_args(subparser):
    subparser.add_argument("-H", "--host", type=str, action="store",
                           required=True, help="psql-server host")
    subparser.add_argument("-U", "--user", type=str, action="store",
                           required=True, help="psql test user")
    subparser.add_argument("-D", "--database", type=str, action="store",
                           required=True, help="psql test database")
    subparser.add_argument("-P", "--password", type=str, action="store",
                           default='', help="psql test password")
    return subparser


def add_subcommand_create(subparsers):
    parser = add_base_args(subparsers.add_parser(
        'graph-create',
        help=subcommand_create.__doc__
    ))
    parser.add_argument(
        "--force", action="store_true",
        help="Hard killing blocking processes that are not in the 'no-kill' list."
    )
    parser.add_argument(
        "--delay", type=int, action="store", default=60,
        help="How many seconds to wait for blocking processes to finish before retrying (and hard killing them if used with --force)."
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

    return_value = {
        'graph-create': subcommand_create,
        'graph-grant': subcommand_grant,
        'graph-revoke': subcommand_revoke,
    }[args.subcommand](args)

    logger.info("Done.")
    return return_value
