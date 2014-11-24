from sqlalchemy import create_engine, select, MetaData, Table, Column, Integer

"""
Driver to implement the graph model in postgres
"""

class NotConnected(Exception):
    pass


class PostgresNode(object):

    def __init__(self, node_id=None, key=None, voided=None, created=None, acl=[], 
                 system_annotations=[], label=None, properties={}):

        self.node_id = node_id
        self.key = key
        self.voided = voided
        self.created = created
        self.acl = acl
        self.system_annotations = system_annotations
        self.label = label
        self.properties = properties
        

class PostgresEdge(object):

    def __init__(self, edge_id=None, key=None, src_id=None, dst_id=None, voided=None, 
                 created=None, acl=[], system_annotations=[], label=None, properties={}):

        self.edge_id = edge_id
        self.key = key
        self.voided = voided
        self.created = created
        self.src_id = src_id
        self.dst_id = dst_id
        self.acl = acl
        self.system_annotations = system_annotations
        self.label = label
        self.properties = properties    


class PostgresGraphDriver(object):
    
    def __init__(self, host, user, password, database):

        self.user = user
        self.database = database
        self.host = host

        self.table = None

        conn_str = 'postgresql://{user}:{password}@{host}/{database}'.format(
            user=user, password=password, host=host, database=database)

        self.engine = create_engine(conn_str)

    def connect_to_table(self, table):
        metadata = MetaData()
        self.table = Table(table, metadata, autoload=True, autoload_with=self.engine)

    def is_connected(self):
        return self.table is not None

    def node_merge(self, node_id=None, matches={}, acl=[], system_annotations=[], label=None, properties={}):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        :param acl: authoritative list that drives object store acl amongst others
        :param system_annotations: the only property to be mutable? This would allow for flexible kv storage tied to the node that does not bloat the database for things like downloaders, and harmonizers
        :param label: this is the node type
        :param properties: the jsonb document containing the node properties
        """

        
        node = PostgresNode()
        return node

    def node_lookup(self, node_id = None, matches = {}, include_voided = False):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        """

        nodes = [PostgresNode()]
        return nodes

    def node_clobber(self, node_id=None, matches={}, acl=[], system_annotations=[], label=None, properties={}):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        :param acl: authoritative list that drives object store acl amongst others
        :param system_annotations: the only property to be mutable? This would allow for flexible kv storage tied to the node that does not bloat the database for things like downloaders, and harmonizers
        :param label: this is the node type
        :param properties: the jsonb document containing the node properties
        """

        node = PostgresNode()
        return node

    def node_delete_property_keys(self, node_id=None, matches={}, keys=[]):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        :param acl: authoritative list that drives object store acl amongst others
        :param system_annotations: the only property to be mutable? This would allow for flexible kv storage tied to the node that does not bloat the database for things like downloaders, and harmonizers
        :param label: this is the node type
        :param properties: the jsonb document containing the node properties
        """

        node = PostgresNode()
        return node

    def node_delete_system_annotation_keys(self, node_id=None, matches={}, keys=[]):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        :param acl: authoritative list that drives object store acl amongst others
        :param system_annotations: the only property to be mutable? This would allow for flexible kv storage tied to the node that does not bloat the database for things like downloaders, and harmonizers
        :param label: this is the node type
        :param properties: the jsonb document containing the node properties
        """

        node = PostgresNode()
        return node

    def node_delete(self, node_id=None, matches={}, acl=[], system_annotations=[], label=None, properties={}):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        :param acl: authoritative list that drives object store acl amongst others
        :param system_annotations: the only property to be mutable? This would allow for flexible kv storage tied to the node that does not bloat the database for things like downloaders, and harmonizers
        :param label: this is the node type
        :param properties: the jsonb document containing the node properties
        """

        node = PostgresNode()
        return node

        
