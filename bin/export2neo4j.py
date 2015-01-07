import logging
from psqlgraph import psqlgraph2neo4j

logging.basicConfig(level=logging.WARN)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mapping = 'translate.yaml'
datatype = 'biospecimen'
host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'


def get_exporter():
    exporter = psqlgraph2neo4j.PsqlGraph2Neo4j()
    exporter.connect_to_psql(host, user, password, database)
    exporter.connect_to_neo4j(host)
    return exporter


def clear_graph(exporter):
    exporter.neo4jDriver.cypher.execute(
        'MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r')

if __name__ == '__main__':
    exporter = get_exporter()
    clear_graph(exporter)
    exporter.export(batch_size=1000)
