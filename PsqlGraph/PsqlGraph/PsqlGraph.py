from sqlalchemy import create_engine, select, MetaData, Table, Column, Integer

"""
Driver to implement the graph model in postgres
"""

class NotConnected(Exception):
    pass

class QueryException(Exception):
    pass

class PostgresNode(object):

    def __init__(self, key, node_id=None, voided=None, created=None, acl=[], 
                 system_annotations=[], label=None, properties={}):

        self.key = key
        self.node_id = node_id
        self.key = key
        self.voided = voided
        self.created = created
        self.acl = acl
        self.system_annotations = system_annotations
        self.label = label
        self.properties = properties
        

class PostgresEdge(object):

    def __init__(self, key, edge_id=None, src_id=None, dst_id=None, voided=None, 
                 created=None, acl=[], system_annotations=[], label=None, properties={}):

        self.key = key
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
        return self.table

    def is_connected(self):
        return self.table is not None

    def node_merge(self, node_id=None, matches=None, acl=[], system_annotations=[], label=None, properties={}):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        :param acl: authoritative list that drives object store acl amongst others
        :param system_annotations: the only property to be mutable? This would allow for flexible kv storage tied to the node that does not bloat the database for things like downloaders, and harmonizers
        :param label: this is the node type
        :param properties: the jsonb document containing the node properties
        """

        self.connect_to_table('nodes')
        node = self.node_lookup_unique(node_id, matches)
        return node

    def node_lookup_unique(self, node_id=None, matches=None, include_voided=False, get_data=False):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        :param get_data: default to ``False`` and will return only the unique key of the node. If set to ``True`` then the lookup will return all node properties
        """
        
        nodes = self.node_lookup(node_id, matches, include_voided, get_data)
        assert len(nodes) == 1, 'Expected a single non-voided node to be found, instead found {count}'.format(count=len(nodes))
        return nodes[0]

    def node_lookup(self, node_id=None, matches=None, include_voided=False, get_data=False):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        """

        if (node_id is not None) and (matches is not None): 
            raise QueryException("Node lookup by node_id and kv matches not accepted")
        if node_id is None and matches is None: 
            raise QueryException("No node_id or kv matches specified")

        if node_id is not None:
            return self._node_lookup_by_id(node_id, include_voided, get_data)

        elif matches is not None:
            return None

    def _node_lookup_by_id(self, node_id=None, include_voided=False, get_properties=False):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        """

        table = self.connect_to_table('nodes')

        if not get_properties:
            query = select([table.c.key]).where(table.c.node_id==node_id)
        else:
            query = select([
                Table.c.node_id, table.c.key, table.c.voided, table.c.created, table.c.acl, 
                table.c.system_annotations, table.c.label, table.c.properties
            ]).where(table.c.node_id==node_id)

        conn = self.engine.connect()
        result = conn.execute(query).fetchall()
        conn.close()

        return result


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

        
