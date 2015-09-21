import argparse
from sqlalchemy import create_engine

from gdcdatamodel.models import *
from psqlgraph import create_all, Node, Edge
import random
from multiprocessing import Process
import time


ID = random.randint(1000, 9999)
name_root = "table_creator_"
name = "{}{}".format(name_root, ID)
connect_args = {"application_name": name}
no_kill_list = []

blocking_SQL = """

SELECT
    blocked_activity.application_name  AS blocked_appname,
    blocked_locks.pid                  AS blocked_pid,
    blocking_activity.application_name AS blocking_appname,
    blocking_locks.pid                 AS blocking_pid,
    blocking_activity.query   AS blocking_statement
FROM  pg_catalog.pg_locks         blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks         blocking_locks
    ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid

JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.GRANTED AND blocked_activity.application_name = '{}';

"""


def create_tables(timeout, host, user, password, database):
    """
    create a table
    """
    print('Creating tables in database')

    engine = create_engine("postgres://{user}:{pwd}@{host}/{db}".format(
        user=user, host=host, pwd=password, db=database
    ), connect_args=connect_args)
    connection = engine.connect()
    trans = connection.begin()
    try:
        connection.execute("SET LOCAL lock_timeout = '{}s';".format(timeout))
        create_all(connection)
        trans.commit()
    except Exception as e:
        trans.rollback()
        if 'timeout' in str(e):
            print 'Attempt timed out'
        else:
            raise


def create_indexes(host, user, password, database):
    print('Creating indexes')
    engine = create_engine("postgres://{user}:{pwd}@{host}/{db}".format(
        user=user, host=host, pwd=password, db=database
    ), connect_args=connect_args)
    index = lambda t, c: ["CREATE INDEX ON {} ({})".format(t, x) for x in c]
    for scls in Node.get_subclasses():
        tablename = scls.__tablename__
        map(engine.execute, index(
            tablename, [
                'node_id',
            ]))
        map(engine.execute, [
            "CREATE INDEX ON {} USING gin (_sysan)".format(tablename),
            "CREATE INDEX ON {} USING gin (_props)".format(tablename),
            "CREATE INDEX ON {} USING gin (_sysan, _props)".format(tablename),
        ])
    for scls in Edge.get_subclasses():
        map(engine.execute, index(
            scls.__tablename__, [
                'src_id',
                'dst_id',
                'dst_id, src_id',
            ]))


def is_blocked_by_no_kill(blocking):
    for proc in blocking:
        blocked_appname, bd_pid, bing_appname, bing_pid, query = proc
        if bing_appname in no_kill_list:
            print 'Blocked by no-kill process {}, {}: {}'.format(
                bing_appname, bing_pid, query)
            return True
    return False


def force_create_tables(delay, retries, host, user, password, database):

    print 'Running table creator named', name
    p = Process(target=create_tables, args=(
        delay*2, host, user, password, database))
    p.start()

    if not p.is_alive():
        return p.join()

    print('Process is blocked, waiting {} seconds for unlock'.format(delay))
    time.sleep(delay)

    # If p has ended, the block was cleared without intervention
    if not p.is_alive():
        print('Process unblocked without killing anything.')
        return p.join()

    # Lookup blocking processes
    engine = create_engine("postgres://{user}:{pwd}@{host}/{db}".format(
        user=args.user, host=args.host, pwd=args.password, db=args.database
    ), connect_args={
        'application_name': 'table_creator_terminator_{}'.format(ID)})
    blocking = list(engine.execute(blocking_SQL.format(name)))

    # Check if any high importance process is blocking us
    if is_blocked_by_no_kill(blocking):
        if retries <= 0:
            raise RuntimeError('Max retries exceeded')
        print('Trying again in {} seconds ({} retries remaining)'.format(
            delay, retries))
        time.sleep(delay)
        return force_create_tables(
            delay, retries-1, host, user, password, database)

    # Kill blocking processes
    for proc in blocking:
        bd_appname, bd_pid, bing_appname, bing_pid, query = proc

        # Skip other table_creators
        if bing_appname.startswith(name_root):
            continue

        # Kill anything in the way, it was deemed of low importance
        print('Killing blocking backend process: name({})\tpid({}): {}'
              .format(bing_appname, bing_pid, query))
        engine.execute('select pg_terminate_backend({});'.format(bing_pid))

    return p.join()


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
    parser.add_argument("--delay", type=int, action="store",
                        default=600, help="How many seconds to wait for blocking processes to finish before hard killing them.")
    parser.add_argument("--retries", type=int, action="store",
                        default=10, help="If blocked by important process, how many times to retry after waiting `delay` seconds.")

    args = parser.parse_args()
    force_create_tables(
        args.delay, args.retries, args.host, args.user, args.password, args.database)
    create_indexes(args.host, args.user, args.password, args.database)
