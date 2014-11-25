import time
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, select, MetaData, Table, Column, Integer, Text, String
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
import uuid
from sqlalchemy.dialects.postgres import *
from pprint import pprint
Base = declarative_base()

"""
Driver to implement the graph model in postgres
"""

class NotConnected(Exception):
    pass

class QueryException(Exception):
    pass

class PsqlNode(Base):

    __tablename__ = 'nodes'

    key = Column(Integer, primary_key=True)
    node_id = Column(String(36), nullable=False)
    voided = Column(TIMESTAMP)
    created = Column(TIMESTAMP, nullable=False, default=datetime.now())
    acl = Column(ARRAY(Text))
    system_annotations = Column(JSONB, default={})
    label = Column(Text)
    properties = Column(JSONB, default={})

    def __repr__(self):
       return "<PostgresNode(key={key}, node_id={node_id}, voided={voided})>".format(
           node_id=self.node_id, key=self.key, voided=(self.voided is not None)
       )

    def merge(self, acl=None, system_annotations=None, label=None, properties=None):

        new_system_annotations = {key: value for key, value in self.system_annotations.iteritems()}
        new_properties = {key: value for key, value in self.properties.iteritems()}
        new_acl = acl[:]
        
        if system_annotations: 
            for key, value in system_annotations.iteritems(): new_system_annotations[key] = value
        if properties: 
            for key, value in properties.iteritems(): new_properties[key] = value
        if acl: new_acl = new_acl + acl

        if not label: label = self.label

        return PsqlNode(node_id=self.node_id, acl=acl, system_annotations=new_system_annotations,
                        label=label, properties=new_properties)


class PsqlEdge(Base):

    __tablename__ = 'edges'

    key = Column(Integer, primary_key=True)
    edge_id = Column(String(36), nullable=False)
    voided = Column(TIMESTAMP)
    created = Column(TIMESTAMP, nullable=False, default=datetime.now())
    src_id = Column(String(36), nullable=False)
    dst_id = Column(String(36), nullable=False)
    acl = Column(ARRAY(Text))
    system_annotations = Column(JSONB, default={})
    label = Column(Text)
    properties = Column(JSONB, default={})

    def __repr__(self):
       return "<PostgresNode(node_id={node_id}, key={key}, acl={acl})>".format(
           node_id=node_id, key=key, acl=acl
       )

class PsqlGraphDriver(object):
    
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

    def node_merge(self, node=None, node_id=None, property_matches=None, system_annotation_matches=None, 
                   acl=[], system_annotations={}, label=None, properties={}):

        if not node:
            node = self.node_lookup_unique(node_id, property_matches, system_annotation_matches)

        if node:
            new_node = node.merge(system_annotations=system_annotations, acl=acl, label=label, properties=properties)
        else:
            if not node_id: node_id = str(uuid.uuid4())
            new_node = PsqlNode(node_id=node_id, system_annotations=system_annotations, acl=acl, label=label, properties=properties)

        self.node_void_and_create(new_node, node)

    def node_void_and_create(self, new_node, old_node):
        """
        This function assumes that you have already done a query for an existing node!
        """

        Session = sessionmaker()
        Session.configure(bind=self.engine)
        session = Session()

        if old_node: 
            old_node.voided = datetime.now()
            voided = session.merge(old_node)
            
        session.add(new_node)
        session.commit()

    def node_lookup_unique(self, node_id=None, property_matches=None, system_annotation_matches=None, include_voided=False):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        """
        
        nodes = self.node_lookup(node_id, property_matches, system_annotation_matches, include_voided)
        assert len(nodes) <= 1, 'Expected a single non-voided node to be found, instead found {count}'.format(count=len(nodes))
        return None if not len(nodes) else nodes[0]

    def node_lookup(self, node_id=None, property_matches=None, system_annotation_matches=None, include_voided=False):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        """

        if not ((node_id is not None) ^ (property_matches is not None or system_annotation_matches is not None)): 
            raise QueryException("Node lookup by node_id and kv matches not accepted")

        if node_id is None and property_matches is None and system_annotation_matches is None: 
            raise QueryException("No node_id or kv matches specified")

        if node_id is not None:
            return self.node_lookup_by_id(node_id, include_voided)

        else:
            return self.node_lookup_by_matches(property_matches, include_voided)

    def node_lookup_by_id(self, node_id, include_voided=False):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        """

        table = self.connect_to_table('nodes')        

        Session = sessionmaker()
        Session.configure(bind=self.engine)
        session = Session()

        query = session.query(PsqlNode).filter(PsqlNode.node_id == node_id)
        if not include_voided: query = query.filter(PsqlNode.voided.is_(None))
        session.commit()

        return query.all()

    def node_lookup_by_matches(self, property_matches=None, system_annotation_matches=None, include_voided=False):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        """

        table = self.connect_to_table('nodes')


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

        
