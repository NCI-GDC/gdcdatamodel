import argparse
from sqlalchemy import create_engine, text

from gdcdatamodel import models as md  # noqa
from psqlgraph import create_all, Node, Edge
import random
from multiprocessing import Process
import time


from gdcdatamodel.models import (
    reports,
    submission,
)


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
WHERE NOT blocked_locks.GRANTED AND blocked_activity.application_name = :name;

"""


def grant_graph_permissions(engine, roles, grant_users):
    for grant_user in grant_users:
        for cls in Node.get_subclasses() + Edge.get_subclasses():
            stmt = "GRANT {roles} ON TABLE {table} TO {user};".format(
                roles=roles, table=cls.__tablename__, user=grant_user
            )
            print(stmt.strip())
            engine.execute(text("BEGIN;" + stmt + "COMMIT;"))


def grant_graph_write_permissions(engine, grant_users):
    grant_graph_permissions(engine, "SELECT,INSERT,UPDATE,DELETE", grant_users)


def grant_graph_read_permissions(engine, grant_users):
    grant_graph_permissions(engine, "SELECT", grant_users)


def create_graph_tables(engine, timeout):
    """
    create a table
    """
    print("Creating tables in database")

    connection = engine.connect()
    trans = connection.begin()
    try:
        connection.execute(
            text("SET LOCAL lock_timeout = :timeout ;"),
            timeout="{}s".format(timeout * 1000),
        )
        create_all(connection)
        trans.commit()
    except Exception as e:
        trans.rollback()
        if "timeout" in str(e):
            print("Attempt timed out", str(e))
        else:
            raise


def create_misc_tables(engine):
    print("Creating submission transaction tables...")
    submission.Base.metadata.create_all(engine)
    print("Creating reporting tables...")
    reports.Base.metadata.create_all(engine)


def is_blocked_by_no_kill(blocking):
    for proc in blocking:
        blocked_appname, bd_pid, bing_appname, bing_pid, query = proc
        if bing_appname in no_kill_list:
            print(
                "Blocked by no-kill process {}, {}: {}".format(
                    bing_appname, bing_pid, query
                )
            )
            return True
    return False


def force_create_graph_tables(engine, delay, retries):

    print("Running table creator named", name)
    p = Process(target=create_graph_tables, args=(engine, delay * 2))
    p.start()

    time.sleep(1)

    if not p.is_alive():
        print("Process not blocked.")
        return p.join()

    else:
        print("Process is blocked, waiting {} seconds for unlock".format(delay))
        time.sleep(delay)

    # If p has ended, the block was cleared without intervention
    if not p.is_alive():
        print("Process unblocked without killing anything.")
        return p.join()

    # Lookup blocking processes
    blocking = list(engine.execute(text(blocking_SQL), name=name))

    # Check if any high importance process is blocking us
    if is_blocked_by_no_kill(blocking):
        if retries <= 0:
            raise RuntimeError("Max retries exceeded")
        print(
            "Trying again in {} seconds ({} retries remaining)".format(delay, retries)
        )
        time.sleep(delay)
        return force_create_graph_tables(engine, delay, retries - 1)

    # Kill blocking processes
    for proc in blocking:
        bd_appname, bd_pid, bing_appname, bing_pid, query = proc

        # Skip other table_creators
        if bing_appname.startswith(name_root):
            continue

        # Kill anything in the way, it was deemed of low importance
        print(
            "Killing blocking backend process: name({})\tpid({}): {}".format(
                bing_appname, bing_pid, query
            )
        )
        engine.execute(
            text("select pg_terminate_backend(:bing_pid);"), bing_pid=bing_pid
        )

    return p.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Driver config
    parser.add_argument(
        "-i",
        "--host",
        type=str,
        action="store",
        default="localhost",
        help="psql-server host",
    )
    parser.add_argument(
        "-u", "--user", type=str, action="store", default="test", help="psql test user"
    )
    parser.add_argument(
        "-p",
        "--password",
        type=str,
        action="store",
        default="test",
        help="psql test password",
    )
    parser.add_argument(
        "-d",
        "--database",
        type=str,
        action="store",
        default="automated_test",
        help="psql database",
    )

    # Graph creation config
    parser.add_argument(
        "--delay",
        type=int,
        action="store",
        default=600,
        help="How many seconds to wait for blocking processes to finish before hard killing them.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        action="store",
        default=10,
        help="If blocked by important process, how many times to retry after waiting `delay` seconds.",
    )

    # Table creation directives
    parser.add_argument(
        "--no-graph-tables", action="store_true", help="Do not create graph tables."
    )
    parser.add_argument(
        "--no-misc-tables", action="store_true", help="Do not create misc tables."
    )

    # Grant privs on graph to users
    parser.add_argument(
        "--graph-write-users",
        type=str,
        action="store",
        default="",
        help="Users to grant write privs to on new graph tables.",
    )
    parser.add_argument(
        "--graph-read-users",
        type=str,
        action="store",
        default="",
        help="Users to grant read privs to on new graph tables.",
    )

    args = parser.parse_args()

    graph_write_users = [u for u in args.graph_write_users.split(",") if u]
    graph_read_users = [u for u in args.graph_read_users.split(",") if u]
    engine = create_engine(
        "postgres://{user}:{pwd}@{host}/{db}".format(
            user=args.user, host=args.host, pwd=args.password, db=args.database
        ),
        connect_args=connect_args,
    )

    # Graph tables
    if not args.no_graph_tables:
        force_create_graph_tables(engine, args.delay, args.retries)
    if args.graph_write_users:
        grant_graph_write_permissions(engine, graph_write_users)
    if args.graph_read_users:
        grant_graph_read_permissions(engine, graph_read_users)

    # Misc tables
    if not args.no_misc_tables:
        create_misc_tables(engine)
