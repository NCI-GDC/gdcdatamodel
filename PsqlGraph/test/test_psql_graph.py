import uuid
import unittest
import time
from cdisutils import log
from PsqlGraph import PostgresGraphDriver
from PsqlGraph.setup_psql_graph import setup_database, create_node_table, create_edge_table, try_drop_test_data

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'
table = 'test'


class TestPostgresGraphSetup(unittest.TestCase):

    def setUp(self):
        self.logger = log.get_logger(__name__)

    def test_setup(self):
        setup_database(user, password, database)
        create_node_table(host, user, password, database)
        create_edge_table(host, user, password, database)

class TestPostgresGraphTeardown(unittest.TestCase):

    def setUp(self):
        self.logger = log.get_logger(__name__)

    def test_teardown(self):
        time.sleep(1)
        try_drop_test_data(user, database)

class TestPostgresGraphDriver(unittest.TestCase):

    def setUp(self):
        self.logger = log.get_logger(__name__)
        self.driver = PostgresGraphDriver(host, user, password, database)

    def test_connect_to_node_table(self):
        self.driver.connect_to_table('nodes')
        self.assertTrue(self.driver.is_connected())

    def test_node_merge_and_lookup(self):
        self.driver.connect_to_table('nodes')

        tempid = str(uuid.uuid4())
        # tempid = u'4f4fa670-8b2c-4cc8-80cc-8212f730343a'
        print tempid
        properties = {'key1':None, 'key2':2, 'key3':time.time()}
        self.driver.node_merge(tempid, properties=properties)

        node = self.driver.node_lookup_unique(tempid, get_data=True)
        self.assertEqual(properties, node.properties)

    def test_node_update(self):
        self.driver.connect_to_table('nodes')

        tempid = str(uuid.uuid4())

        propertiesA = {'key1':None, 'key2':2, 'key3':time.time()}
        self.driver.node_merge(tempid, properties=propertiesA)

        propertiesB = {'key1':None, 'new_key':2, 'timestamp':time.time()}
        self.driver.node_merge(tempid, properties=propertiesB)

        node = self.driver.node_lookup_unique(tempid, get_data=True)
        self.assertEqual(properties, node.properties, 'Node properties do not match expected properties')

        nodes = self.driver.node_lookup(tempid, include_voided=True, get_data=True)
        self.assertEqual(len(nodes), 1, 'Expected a single voided node to be found, instead found {count}'.format(count=len(nodes)))
        self.assertEqual(properties, nodes[0].properties, 'Node properties do not match expected properties')

    def test_repeated_node_update(self):
        self.driver.connect_to_table('nodes')

        update_count = 5000
        tempid = str(uuid.uuid4())

        for tally in range(update_count):
            properties = {'key1':None, 'key2':2, 'key3':time.time(), 'tally': tally}
            self.driver.node_merge(tempid, properties=properties)

        nodes = self.driver.node_lookup(tempid, get_data=True)
        self.assertEqual(len(nodes), 1, 'Expected a single non-voided node to be found, instead found {count}'.format(count=len(nodes)))
        self.assertEqual(properties, nodes[0].properties, 'Node properties do not match expected properties')

        nodes = self.driver.node_lookup(tempid, voided=True, get_data=True)
        self.assertEqual(len(nodes), update_count - 1, 'Expected a {update_count} voided nodes to be found, instead found {count}'.format(
                        update_count=update_count, count=len(nodes)))

    def test_node_clobber(self):

        tempid = str(uuid.uuid4())

        propertiesA = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.node_merge(tempid, properties=propertiesA)

        propertiesB = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.node_clobber(tempid, properties=propertiesB)

        nodes = self.driver.node_lookup(tempid, get_data=True)
        self.assertEqual(len(nodes), 1, 'Expected a single node to be found, instead found {count}'.format(count=len(nodes)))
        self.assertEqual(propertiesB, nodes[0].properties, 'Node properties do not match expected properties')

    def test_node_delete_property_keys(self):

        tempid = str(uuid.uuid4())
        properties = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.node_merge(tempid, properties=properties)

        self.driver.node_delete_property_keys(tempid, ['key2', 'key3'])
        properties.pop('key2')
        properties.pop('key3')

        nodes = self.driver.node_lookup(tempid, get_data=True)
        self.assertEqual(len(nodes), 1, 'Expected a single node to be found, instead found {count}'.format(count=len(nodes)))
        self.assertEqual(properties, nodes[0].properties)

    def test_node_delete_system_annotation_keys(self):

        tempid = str(uuid.uuid4())
        annotations = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.node_merge(tempid, system_annotations=annotations)

        self.driver.node_delete_system_annotations_keys(tempid, ['key2', 'key3'])

        annotations.pop('key2')
        annotation.pop('key3')

        nodes = self.driver.node_lookup(tempid, get_data=True)
        self.assertEqual(len(nodes), 1, 'Expected a single node to be found, instead found {count}'.format(count=len(nodes)))
        self.assertEqual(properties, nodes[0].properties)

    def test_node_delete(self):

        tempid = str(uuid.uuid4())

        self.driver.node_merge(tempid)
        self.driver.node_delete(tempid)

        nodes = self.driver.node_lookup(tempid, get_data=True)
        self.assertEqual(len(nodes), 0, 'Expected a no non-voided nodes to be found, instead found {count}'.format(count=len(nodes)))

        nodes = self.driver.node_lookup(tempid, get_data=True)
        self.assertEqual(len(nodes), 0, 'Expected a single no non-voided nodes to be found, instead found {count}'.format(count=len(nodes)))

    def test_repeated_node_delete(self):

        tempid = str(uuid.uuid4())
        void_count = 5

        for i in range(void_count):
            self.driver.node_merge(tempid)
            self.driver.node_delete(tempid)

        nodes = self.driver.node_lookup(tempid, include_voided=False, get_data=True)
        self.assertEqual(len(nodes), 0, 'Expected a no non-voided nodes to be found, instead found {count}'.format(count=len(nodes)))

        nodes = self.driver.node_lookup(tempid, include_voided=True, get_data=True)
        self.assertEqual(len(nodes), void_count - 1, 'Expected a single {exp} non-voided nodes to be found, instead found {real}'.format(
                        exp=void_count, real=len(nodes)))

    def test_edge_merge_and_lookup(self):
        self.driver.connect_to_table('edges')

        tempid = str(uuid.uuid4())
        properties = {'key1':None, 'key2':2, 'key3':time.time()}
        self.driver.edge_merge(tempid, properties=properties)

        edges = self.driver.edge_lookup(tempid, get_data=True)
        self.assertEqual(len(edges), 1, 'Expected a single edge to be found, instead found {count}'.format(count=len(edges)))
        assertTrue(cmp(properties, edges[0].properties) == 0)

    def test_edge_update(self):
        self.driver.connect_to_table('edges')

        tempid = str(uuid.uuid4())

        propertiesA = {'key1':None, 'key2':2, 'key3':time.time()}
        self.driver.edge_merge(tempid, properties=propertiesA)

        propertiesB = {'key1':None, 'new_key':2, 'timestamp':time.time()}
        self.driver.edge_merge(tempid, properties=propertiesB)

        edges = self.driver.edge_lookup(tempid, get_data=True)
        self.assertEqual(len(edges), 1, 'Expected a single non-voided edge to be found, instead found {count}'.format(count=len(edges)))
        self.assertEqual(properties, edges[0].properties, 'Edge properties do not match expected properties')

        edges = self.driver.edge_lookup(tempid, include_voided=True, get_data=True)
        self.assertEqual(len(edges), 1, 'Expected a single voided edge to be found, instead found {count}'.format(count=len(edges)))
        self.assertEqual(properties, edges[0].properties, 'Edge properties do not match expected properties')

    def test_repeated_edge_update(self):
        self.driver.connect_to_table('edges')

        update_count = 5000

        tempid = str(uuid.uuid4())

        for tally in range(update_count):
            properties = {'key1':None, 'key2':2, 'key3':time.time(), 'tally': tally}
            self.driver.edge_merge(tempid, properties=properties)

        edges = self.driver.edge_lookup(tempid, get_data=True)
        self.assertEqual(len(edges), 1, 'Expected a single non-voided edge to be found, instead found {count}'.format(count=len(edges)))
        self.assertEqual(properties, edges[0].properties, 'Edge properties do not match expected properties')

        edges = self.driver.edge_lookup(tempid, voided=True, get_data=True)
        self.assertEqual(len(edges), update_count - 1, 'Expected a {update_count} voided edges to be found, instead found {count}'.format(
                        update_count=update_count, count=len(edges)))

    def test_edge_clobber(self):

        tempid = str(uuid.uuid4())

        propertiesA = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.edge_merge(tempid, properties=propertiesA)

        propertiesB = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.edge_clobber(tempid, properties=propertiesB)

        edges = self.driver.edge_lookup(tempid, get_data=True)
        self.assertEqual(len(edges), 1, 'Expected a single edge to be found, instead found {count}'.format(count=len(edges)))
        self.assertEqual(propertiesB, edges[0].properties, 'Edge properties do not match expected properties')

    def test_edge_delete_property_keys(self):

        tempid = str(uuid.uuid4())
        properties = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.edge_merge(tempid, properties=properties)

        self.driver.edge_delete_property_keys(tempid, ['key2', 'key3'])
        properties.pop('key2')
        properties.pop('key3')

        edges = self.driver.edge_lookup(tempid, get_data=True)
        self.assertEqual(len(edges), 1, 'Expected a single edge to be found, instead found {count}'.format(count=len(edges)))
        self.assertEqual(properties, edges[0].properties)

    def test_edge_delete_system_annotation_keys(self):

        tempid = str(uuid.uuid4())
        annotations = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.edge_merge(tempid, system_annotations=annotations)

        self.driver.edge_delete_system_annotations_keys(tempid, ['key2', 'key3'])

        annotations.pop('key2')
        annotation.pop('key3')

        edges = self.driver.edge_lookup(tempid, get_data=True)
        self.assertEqual(len(edges), 1, 'Expected a single edge to be found, instead found {count}'.format(count=len(edges)))
        self.assertEqual(properties, edges[0].properties)

    def test_edge_delete(self):

        tempid = str(uuid.uuid4())

        self.driver.edge_merge(tempid)
        self.driver.edge_delete(tempid)

        edges = self.driver.edge_lookup(tempid, get_data=True)
        self.assertEqual(len(edges), 0, 'Expected a no non-voided edges to be found, instead found {count}'.format(count=len(edges)))

        edges = self.driver.edge_lookup(tempid, get_data=True)
        self.assertEqual(len(edges), 0, 'Expected a single no non-voided edges to be found, instead found {count}'.format(count=len(edges)))

    def test_repeated_edge_delete(self):

        tempid = str(uuid.uuid4())
        void_count = 5

        for i in range(void_count):
            self.driver.edge_merge(tempid)
            self.driver.edge_delete(tempid)

        edges = self.driver.edge_lookup(tempid, include_voided=False, get_data=True)
        self.assertEqual(len(edges), 0, 'Expected a no non-voided edges to be found, instead found {count}'.format(count=len(edges)))

        edges = self.driver.edge_lookup(tempid, include_voided=True, get_data=True)
        self.assertEqual(len(edges), void_count - 1, 'Expected a single {exp} non-voided edges to be found, instead found {real}'.format(
                        exp=void_count, real=len(edges)))


if __name__ == '__main__':
    
    def run_test(test):
        suite = unittest.TestLoader().loadTestsFromTestCase(test)
        unittest.TextTestRunner(verbosity=2).run(suite)

    run_test(TestPostgresGraphSetup)
    run_test(TestPostgresGraphDriver)
    # run_test(TestPostgresGraphTeardown)
