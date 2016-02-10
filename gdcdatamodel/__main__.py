import argparse
import getpass
import psqlgraph

from models import *                              # noqa
from psqlgraph import *                           # noqa
from sqlalchemy import *                          # noqa
from models.versioned_nodes import VersionedNode  # noqa

try:
    import IPython
    ipython = True
except Exception as e:
    print(('{}, using standard interactive console. '
           'If you install IPython, then it will automatically '
           'be used for this repl.').format(e))
    import code
    ipython = False


message = """
Entering psqlgraph console:
    database : {}
    host     : {}
    user     : {}

NOTE:
    PsqlGraphDriver stored in local variable `g`.
    A `g.session_scope` session is at `s`.
    `rb()` will rollback the session.
"""


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', default='test', type=str,
                        help='name of the database to connect to')
    parser.add_argument('-i', '--host', default='localhost', type=str,
                        help='host of the postgres server')
    parser.add_argument('-u', '--user', default='test', type=str,
                        help='user to connect to postgres as')
    parser.add_argument('-p', '--password', default=None, type=str,
                        help='password for given user. If no '
                        'password given, one will be prompted.')

    args = parser.parse_args()

    print(message.format(args.database, args.host, args.user))
    if args.password is None:
        args.password = getpass.getpass()

    g = psqlgraph.PsqlGraphDriver(
        args.host, args.user, args.password, args.database)

    with g.session_scope() as s:
        rb = s.rollback
        if ipython:
            IPython.embed()
        else:
            code.InteractiveConsole(locals=globals()).interact()
