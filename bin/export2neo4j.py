import logging
from psqlgraph import psqlgraph2neo4j
from getpass import getpass
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.root.setLevel(level=logging.WARN)

base_url = 'http://{host}:{port}/load2neo/load/geoff'

host = 'localhost'
user = 'test'
password = 'test'
database = 'gdc_datamodel'

"""
MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r
"""


def get_exporter(args):
    exporter = psqlgraph2neo4j.PsqlGraph2Neo4j()
    exporter.connect_to_psql(
        args.host, args.user, args.password, args.database)
    exporter.connect_to_neo4j(args.neo_host, args.neo_port)
    return exporter

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
    parser.add_argument("--neo-host", type=str, action="store",
                        default='localhost', help="neo4j server host")
    parser.add_argument("--neo-port", type=str, action="store",
                        default='7474', help="neo4j server port")
    args = parser.parse_args()
    if not args.password:
        args.password = getpass('psqlgraph password: ')
    exporter = get_exporter(args)
    exporter.drop_neo4j_data()
    exporter.export()
