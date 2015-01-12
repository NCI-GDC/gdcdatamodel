import logging
from psqlgraph import psqlgraph2geoff

logging.basicConfig(level=logging.WARN)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mapping = 'translate.yaml'
datatype = 'biospecimen'
host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'

"""
MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r
"""


def get_exporter():
    return psqlgraph2geoff.PsqlGraph2Geoff(
        host, user, password, database)


if __name__ == '__main__':
    exporter = get_exporter()
    with open('test.geoff', 'w') as output_file:
        exporter.export(output_file)
