#!/usr/bin/env python
from psqlgraph import PsqlGraphDriver, Node
import argparse
from sqlalchemy import BigInteger, func

from gdcdatamodel.models import File

"""This is a simple script for printing the downloaded and total
amounts of data for [tcga, target] x [[cghub], [dcc]].

"""


def base_query(g, state):
    """Query for all files in a state

    """

    return g.nodes(File).props(dict(state=state))


def end_query(q):
    """Take a query, and sum all values, assuming that's what you're doing
    in this script because that's all you should be doing in this
    script, and return the value in TB (base 10).

    """

    try:
        return float(q.with_entities(func.sum(
            Node.properties['file_size'].cast(BigInteger))).all()[0][0])/1e12
    except:
        return 0


def query_total(g, state):
    """Get the total size in TB of all files in a given state

    """

    return end_query(base_query(g, state))


def query_size(g, source, state):
    """Get the total size in TB of all files in a given state from a given
    source.

    """

    return end_query(base_query(g, state).sysan(dict(source=source)))


def print_project_source(g, project, source):
    """Get the total size downloaded and total data in TB of all files
    in a given state from a given source.

    """

    name = '{}_{}'.format(project, source)
    live = query_size(g, name, 'live')
    submitted = query_size(g, name, 'submitted')

    print '{} {} Data'.format(project.upper(), source.upper())
    print 'Total Downloaded\t{}'.format(live)
    print 'Total Data\t{}'.format(submitted+live)


def print_total(g):
    """Print the totals of downloaded and total data

    """

    live = query_total(g, 'live')
    submitted = query_total(g, 'submitted')

    print 'Total Downloaded\t{}'.format(live)
    print 'Total Data\t{}'.format(submitted+live)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', required=True, type=str,
                        help='name of the database to connect to')
    parser.add_argument('-i', '--host', required=True, type=str,
                        help='host of the postgres server')
    parser.add_argument('-u', '--user', required=True, type=str,
                        help='user to connect to postgres as')
    parser.add_argument('-p', '--password', required=True, type=str,
                        help='password for given user. If no '
                        'password given, one will be prompted.')
    args = parser.parse_args()

    g = PsqlGraphDriver(**args.__dict__)

    with g.session_scope() as session:
        for project in {'tcga', 'target'}:
            for source in {'cghub', 'dcc'}:
                print_project_source(g, project, source)
        print_total(g)
