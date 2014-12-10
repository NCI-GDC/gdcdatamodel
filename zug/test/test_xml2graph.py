import unittest
import logging
from zug.submodules import xml2psqlgraph
from psqlgraph import psqlgraph2neo4j

mapping = 'translate.yaml'
datatype = 'biospecimen'
host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'


logging.basicConfig(level=logging.INFO)


class TestPsqlGraphDriver(unittest.TestCase):
    def setUp(self):

        self.logger = logging.getLogger(__name__)
        self.driver = xml2psqlgraph.xml2psqlgraph(
            translate_path=mapping, data_type=datatype, host=host,
            user=user, password=password, database=database)
        self.exporter = psqlgraph2neo4j.PsqlGraph2Neo4j()
        self.exporter.connect_to_psql(host, user, password, database)
        self.exporter.connect_to_neo4j(host)
        self.neo4jDriver = self.exporter.neo4jDriver

    def _clear_graph(self):
        # clear neo4j
        self.neo4jDriver.cypher.execute(
            """MATCH (n:test)
            OPTIONAL MATCH (n:test)-[r]-()
            DELETE n,r
            """
        )
        self.neo4jDriver.cypher.execute(
            """MATCH (n:test)
            OPTIONAL MATCH (n:test2)-[r]-()
            DELETE n,r
            """
        )

    def _test_xml(self, xml):
        self.driver.parse(xml)

    def test_sample_xml(self):
        with open('sample.xml') as f:
            self._test_xml(f.read())

    def test_sample_export(self):
        self._clear_graph()
        self.test_sample_xml()
        self.exporter.export()


if __name__ == '__main__':
    pass
