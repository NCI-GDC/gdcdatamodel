import logging
from psqlgraph import psqlgraph2geoff
import StringIO
import urllib2
import argparse

logging.basicConfig(level=logging.WARN)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

base_url = 'http://{host}:{port}/load2neo/load/geoff'

host = 'localhost'
user = 'test'
password = 'test'
database = 'gdc_datamodel'

"""
MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r
"""


def get_exporter():
    return psqlgraph2geoff.PsqlGraph2Geoff(
        host, user, password, database)


def export_geoff(host, port, geoff):
    print('Exporting to neo4j')
    url = base_url.format(host=host, port=port)
    request = urllib2.Request(url, geoff)
    request.add_header("Content-Type", "application/zip")
    response = urllib2.urlopen(request)
    print response

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--neo-host", type=str, action="store",
                        default='localhost', help="neo4j server host")
    parser.add_argument("--neo-port", type=str, action="store",
                        default='7474', help="neo4j server port")
    args = parser.parse_args()
    export_geoff(args.neo_host, args.neo_port, get_exporter().export())
