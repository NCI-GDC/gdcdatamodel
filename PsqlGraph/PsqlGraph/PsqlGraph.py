from sqlalchemy import create_engine, select, MetaData, Table, Column, Integer
from datetime import datetime

"""
Driver to implement the graph model in postgres
"""

class NotConnected(Exception):
    pass

class QueryException(Exception):
    pass

class PostgresNode(object):

    def __init__(self, kwargs):

        self.key                = kwargs.key if kwargs.has_key('key') else None
        self.node_id            = kwargs.node_id if kwargs.has_key('node_id') else None
        self.voided             = kwargs.voided if kwargs.has_key('voided') else None
        self.created            = kwargs.created if kwargs.has_key('created') else None
        self.acl                = kwargs.acl if kwargs.has_key('acl') else None
        self.system_annotations = kwargs.system_annotations if kwargs.has_key('system_annotations') else None
        self.label              = kwargs.label if kwargs.has_key('label') else None
        self.properties         = kwargs.properties if kwargs.has_key('properties') else None
        

class PostgresEdge(object):

    def __init__(self, key, edge_id=None, src_id=None, dst_id=None, voided=None, 
                 created=None, acl=[], system_annotations={}, label=None, properties={}):

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

    def _merge_values(self, old, new):

        if old is None: return new

        if type(old) != type(new):
            raise Exception('Cannot merge values of type {new} onto {old}'.format(new=type(new), old=type(old)))

        if isinstance(old, list):
            old += new
            return old

        elif isinstance(old, dict):
            for key, value in new.iteritems():
                old[key] = value
            return old
            
        if new is not None: return new

        return new

    def node_merge(self, node_id=None, property_matches=None, system_annotation_matches=None, 
                   acl=[], system_annotations={}, label=None, properties={}):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        :param acl: authoritative list that drives object store acl amongst others
        :param system_annotations: the only property to be mutable? This would allow for flexible kv storage tied to the node that does not bloat the database for things like downloaders, and harmonizers
        :param label: this is the node type
        :param properties: the jsonb document containing the node properties
        """

        node = self.node_lookup_unique(node_id, property_matches, system_annotation_matches, get_data=True)

        if node:
            properties = self._merge_values(node.properties, properties)
            system_annotations = self._merge_values(node.system_annotations, system_annotations)
            acl = self._merge_values(node.acl, acl)
            label = self._merge_values(node.label, label)

        self._node_insert(node_id, acl, system_annotations, label, properties)


    def _node_insert(self, node_id=None, acl=[], system_annotations={}, label=None, properties={}):
        table = self.connect_to_table('nodes')

        ins = table.insert().returning(table.c.node_id).values(
            node_id=node_id,
            acl=acl,
            properties=properties,
            created=datetime.now(),
            system_annotations=system_annotations,
            label=label,
        )

        conn = self.engine.connect()
        result = conn.execute(ins).fetchall()
        conn.close()

    def node_lookup_unique(self, node_id=None, property_matches=None, system_annotation_matches=None, include_voided=False, get_data=False):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        :param get_data: default to ``False`` and will return only the unique key of the node. If set to ``True`` then the lookup will return all node properties
        """
        
        nodes = self.node_lookup(node_id, property_matches, system_annotation_matches, include_voided, get_data)
        assert len(nodes) <= 1, 'Expected a single non-voided node to be found, instead found {count}'.format(count=len(nodes))
        return None if not len(nodes) else nodes[0]

    def node_lookup(self, node_id=None, property_matches=None, system_annotation_matches=None, include_voided=False, get_data=False):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        """

        if not ((node_id is not None) ^ (property_matches is not None or system_annotation_matches is not None)): 
            raise QueryException("Node lookup by node_id and kv matches not accepted")

        if node_id is None and property_matches is None and system_annotation_matches is None: 
            raise QueryException("No node_id or kv matches specified")

        if node_id is not None:
            return self.node_lookup_by_id(node_id, include_voided, get_data)

        else:
            return self.node_lookup_by_matches(property_matches, include_voided, get_data)

    def node_lookup_by_id(self, node_id, include_voided=False, get_data=False):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        """

        table = self.connect_to_table('nodes')        

        if get_data:
            returning = [ 
                table.c.node_id, table.c.key, table.c.voided, table.c.created, table.c.acl, 
                table.c.system_annotations, table.c.label, table.c.properties 
            ]
        else: returning = [table.c.key]

        query = select(returning).where(table.c.node_id==node_id)
        if not include_voided: query = query.where(table.c.voided==None)

        conn = self.engine.connect()

        results = conn.execute(query).fetchall()
        conn.close()
        
        nodes = [PostgresNode(row) for row in results]
        
        return nodes

    def node_lookup_by_matches(self, property_matches=None, system_annotation_matches=None, include_voided=False, get_data=False):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        """

        table = self.connect_to_table('nodes')

        if get_data:
            returning = [ 
                Table.c.node_id, table.c.key, table.c.voided, table.c.created, table.c.acl, 
                table.c.system_annotations, table.c.label, table.c.properties 
            ]
        else: returning = [table.c.key]

        query = select(returning)
        
        if property_matches:
            for key, value in property_matches.iteritems(): 
                qeury = query.where(table.c.properties[key]==value)

        if system_annotation_matches:
            for key, value in syste_annotation_matches.iteritems(): 
                qeury = query.where(table.c.properties[key]==value)

        conn = self.engine.connect()
        result = conn.execute(query).fetchall()
        conn.close()

        return result


    def node_clobber(self, node_id=None, matches={}, acl=[], system_annotations={}, label=None, properties={}):
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

    def node_delete(self, node_id=None, matches={}, acl=[], system_annotations={}, label=None, properties={}):
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

        
