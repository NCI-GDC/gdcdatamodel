from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, \
    Integer, Text, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgres import TIMESTAMP, ARRAY, JSONB
from sqlalchemy.exc import IntegrityError
from contextlib import contextmanager
from datetime import datetime
import time
import random
import logging
import copy

Base = declarative_base()

"""
Driver to implement the graph model in postgres
"""


class NotConnected(Exception):
    pass


class QueryException(Exception):
    pass


class NodeCreationException(Exception):
    pass


@contextmanager
def session_scope(engine, session=None):
    """Provide a transactional scope around a series of operations."""

    if not session:
        Session = sessionmaker(expire_on_commit=False)
        Session.configure(bind=engine)
        local = Session()
        logging.info('Created session {session}'.format(session=local))
    else:
        local = session

    try:
        yield local
        if not session:
            logging.info('Committing session {session}'.format(session=local))
            local.commit()

    except Exception, msg:
        logging.error('Failed to commit session: {msg}'.format(msg=msg))
        logging.error('Rolling back session {session}'.format(session=local))
        local.rollback()
        raise

    finally:
        if not session:

            logging.info('Expunging objects from {session}'.format(
                session=local))
            local.expunge_all()
            logging.info('Closing session {session}'.format(session=local))
            local.close()


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
        return "<PostgresNode(key={key}, node_id={node_id}, voided={voided})>"
        "".format(
            node_id=self.node_id,
            key=self.key,
            voided=(self.voided is not None)
        )

    def merge(self, node):

        new_system_annotations = copy.copy(self.system_annotations)
        new_properties = copy.copy(self.properties)
        new_acl = self.acl[:] + (node.acl or [])
        new_label = (node.label or self.label)

        new_system_annotations.update(node.system_annotations or {})
        new_properties.update(node.properties or {})

        return PsqlNode(
            node_id=self.node_id,
            acl=new_acl,
            system_annotations=new_system_annotations,
            label=new_label,
            properties=new_properties,
        )


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
        return "<PostgresNode(node_id={node_id}, key={key}, acl={acl})>"
        "".format(
            node_id=self.node_id,
            key=self.key,
            acl=self.acl
        )


DEFAULT_RETRIES = 32


def default_backoff(retries, max_retries):
    """This is the default backoff function used in the case of a retry by
    and function wrapped with the ``@retryable`` decorator.

    The behavior of the default backoff function is to sleep for a
    pseudo-random time between 0 and 2 seconds.

    """

    time.sleep(random.random()*(max_retries-retries)/max_retries*2)


def retryable(func, backoff=default_backoff):
    """

    This wrapper can be used to decorate a function to retry an
    operation in the case of an SQLalchemy IntegrityError.  This error
    means that a race-condition has occured and operations that have
    occured within the session may no longer be valid.

    You can set the number of retries by passing the keyword argument
    ``max_retries`` to the wrapped function.  It's therefore important
    that ``max_retries`` is included as a kwarg in the definition of
    the wrapped function.

    Setting ``max_retries`` to 0 will prevent retries upon failure;
    wrapped function will execute once.

    Similar to ``max_retries``, the kwarg ``backoff`` is a callback
    function that allows the user of the library to over-ride the
    default backoff function in the case of a retry.  See `func
    default_backoff`

    """

    def inner(*args, **kwargs):
        retries = 0
        max_retries = kwargs.get('max_retries', DEFAULT_RETRIES)
        backoff = kwargs.get('backoff', default_backoff)
        while retries <= max_retries:
            try:
                func(*args, **kwargs)
            except IntegrityError:
                logging.warn('Race-condition caught ({0}/{1} retries)'
                             ''.format(retries, max_retries))
                if retries >= max_retries:
                    logging.error('Max retries exceeded')
                    raise
                retries += 1
                backoff(retries, max_retries)
            else:
                break
    return inner


class PsqlGraphDriver(object):

    def __init__(self, host, user, password, database):

        self.user = user
        self.database = database
        self.host = host
        self.logger = logging.getLogger('PsqlGraph')
        self.default_retries = 10

        conn_str = 'postgresql://{user}:{password}@{host}/{database}'.format(
            user=user, password=password, host=host, database=database)

        self.engine = create_engine(conn_str)

    @retryable
    def node_merge(self, node=None, node_id=None, property_matches=None,
                   system_annotation_matches=None, acl=[],
                   system_annotations={}, label=None, properties={},
                   session=None,
                   max_retries=DEFAULT_RETRIES,
                   backoff=default_backoff):
        """

        This is meant to be the main interaction function with the
        library. It handles the traditional get_one_or_create while
        overloading the merging of properties, system_annotations.

        - If the node does not exist, it will be created.

        - If the node does exist, it will be updated.

        - This function is thread safe.

        - This function is wrapped by ``retryable`` decorator, set the
          number of maximum number of times that the merge will retry
          after a concurrency error, set the keyword arg ``max_retries``

        """

        self.logger.info('Merging node')

        with session_scope(self.engine, session) as local:
            if not node:
                """ try and lookup the node """
                node = self.node_lookup_one(
                    node_id=node_id,
                    property_matches=property_matches,
                    system_annotation_matches=system_annotation_matches,
                    session=local,
                )

            if node:
                """ there is a pre-existing node """
                new_node = node.merge(PsqlNode(
                    system_annotations=system_annotations,
                    acl=acl,
                    label=label,
                    properties=properties
                ))

            else:
                """ we need to create a new node """
                self.logger.info('Creating a new node')

                if not node_id:
                    raise NodeCreationException(
                        'Cannot create a node with no node_id')

                new_node = PsqlNode(
                    node_id=node_id,
                    system_annotations=system_annotations,
                    acl=acl,
                    label=label,
                    properties=properties
                )

            self.node_void_and_create(new_node, node, session=local)

    def node_void_and_create(self, new_node, old_node, session=None):
        """
        This function assumes that you have already done a query for an
        existing node!  This function will take an node, void it and
        create a new node entry in its place
        """

        self.logger.info('Voiding a node to create a new one')

        with session_scope(self.engine, session) as local:
            try:
                self.node_void(old_node, session)
                local.add(new_node)
            except:
                raise

    def node_void(self, node, session=None):
        """if passed a non-null node, then ``node_void`` will set the
        timestamp on the voided column of the entry.

        If a session is provided, then this action will be carried out
        within the session (and not commited).

        If no session is provided, this function will commit to the
        database in a self-contained session.

        """
        if not node:
            return

        with session_scope(self.engine, session) as local:
            node.voided = datetime.now()
            local.merge(node)

    def node_lookup_one(self, node_id=None, property_matches=None,
                        system_annotation_matches=None, include_voided=False,
                        session=None):
        """
        This function is simply a wrapper for ``node_lookup`` that
        constrains the query to return a single node.  If multiple
        nodes are found matchin the query, an exception will be raised

        If no nodes match the query, the return will be NoneType.
        """
        nodes = self.node_lookup(
            node_id=node_id,
            property_matches=property_matches,
            system_annotation_matches=system_annotation_matches,
            include_voided=include_voided,
            session=session,
        )

        if len(nodes) > 1:
            raise QueryException('Expected a single result for query, got {n}'
                                 ''.format(n=len(nodes)))
        if len(nodes) < 1:
            return None

        return nodes[0]

    def node_lookup(self, node_id=None, property_matches=None,
                    system_annotation_matches=None, include_voided=False,
                    session=None):
        """
        This function wraps both ``node_lookup_by_id`` and
        ``node_lookup_by_matches``. If matches are provided then the
        nodes will be queried by id. If id is provided, then the nodes
        will be queried by id.  Providing both id and matches will be
        treated as an invalid query.
        """

        if not ((node_id is not None) ^
                (property_matches is not None or
                 system_annotation_matches is not None)):
            raise QueryException("Node lookup by node_id and kv matches not"
                                 " accepted.")

        if ((not node_id) and (not property_matches) and
                (not system_annotation_matches)):
            raise QueryException("No node_id or matches specified")

        if node_id is not None:
            return self.node_lookup_by_id(
                node_id=node_id,
                include_voided=include_voided,
                session=session,
            )

        else:
            return self.node_lookup_by_matches(
                property_matches,
                include_voided
            )

    def node_lookup_by_id(self, node_id, include_voided=False, session=None):
        """
        This function looks up a node by a given id.  If include_voided is
        true, then the returned list will include nodes that have been
        voided. If one is true then the return will be constrained to
        a single result (if more than a single result is found, then
        an exception will be raised.  If session is specified, the
        query will be performed within the givin session, otherwise a
        new one will be created.
        """

        self.logger.info('Looking up node by id: {id}'.format(id=node_id))

        with session_scope(self.engine, session) as local:
            query = local.query(PsqlNode).filter(PsqlNode.node_id == node_id)

            if not include_voided:
                query = query.filter(PsqlNode.voided.is_(None))

            result = query.all()

        return result

    def node_lookup_by_matches(self, property_matches=None,
                               system_annotation_matches=None,
                               include_voided=False):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        """

        raise NotImplementedError()

    def node_clobber(self, node_id=None, matches={}, acl=[],
                     system_annotations={}, label=None, properties={}):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        :param acl: authoritative list that drives object store acl amongst others
        :param system_annotations: the only property to be mutable? This would allow for flexible kv storage tied to the node that does not bloat the database for things like downloaders, and harmonizers
        :param label: this is the node type
        :param properties: the jsonb document containing the node properties
        """

        raise NotImplementedError()

    def node_delete_property_keys(self, node_id=None, matches={}, keys=[]):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        :param acl: authoritative list that drives object store acl amongst others
        :param system_annotations: the only property to be mutable? This would allow for flexible kv storage tied to the node that does not bloat the database for things like downloaders, and harmonizers
        :param label: this is the node type
        :param properties: the jsonb document containing the node properties
        """

        raise NotImplementedError()

    def node_delete_system_annotation_keys(self, node_id=None, matches={}, keys=[]):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        :param acl: authoritative list that drives object store acl amongst others
        :param system_annotations: the only property to be mutable? This would allow for flexible kv storage tied to the node that does not bloat the database for things like downloaders, and harmonizers
        :param label: this is the node type
        :param properties: the jsonb document containing the node properties
        """

        raise NotImplementedError()

    def node_delete(self, node_id=None, matches={}, acl=[], system_annotations={}, label=None, properties={}):
        """
        :param node_id: unique id that is only important inside postgres, referenced by edges
        :param matches: key-values to match node if node_id is not provided
        :param acl: authoritative list that drives object store acl amongst others
        :param system_annotations: the only property to be mutable? This would allow for flexible kv storage tied to the node that does not bloat the database for things like downloaders, and harmonizers
        :param label: this is the node type
        :param properties: the jsonb document containing the node properties
        """

        raise NotImplementedError()
