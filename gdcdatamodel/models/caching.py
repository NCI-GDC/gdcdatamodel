# -*- coding: utf-8 -*-
"""gdcdatamodel.models.caching
----------------------------------

Handle information cached on nodes about releationships and other
graph properties.  This cached information is specific to the current
GDC datamodel.


**cache.related_cases**:
    It is important for many transactions to know the ancestor cases
    of a node, so cache a list of related case ids and update the
    cache when the set of graph edges is modified.  The value of this
    key in sysan should be a list of case ids

"""

from cdisutils.log import get_logger
from psqlgraph import Node, Edge
from sqlalchemy.orm.session import make_transient

logger = get_logger('gdcdatamodel')


# This variable specifies the key under which the node_id of related
# cases is cached.
CASE_CACHE_KEY = 'cache.related.case.id'


def get_edge_src(edge):
    """Return the edge's source or None.

    Attempts to lookup the node by id if the association proxy is not
    set.

    """

    if edge.src:
        src = edge.src
    elif edge.src_id is not None:
        src_class = Node.get_subclass_named(edge.__src_class__)
        src = (edge.get_session().query(src_class)
               .filter(src_class.node_id == edge.src_id)
               .first())
    else:
        src = None
    return src


def get_edge_dst(edge, allow_query=False):
    """Return the edge's destination or None.

    """

    if edge.dst:
        dst = edge.dst
    elif edge.dst_id is not None and allow_query:
        dst_class = Node.get_subclass_named(edge.__dst_class__)
        dst = (edge.get_session().query(dst_class)
               .filter(dst_class.node_id == edge.dst_id)
               .first())
    else:
        dst = None

    return dst


def related_cases_from_cache(node):
    """Get the cached related case ids from this node's sysan

    :param node: The Node instance
    :returns: List of str ids

    """

    return node.sysan.get(CASE_CACHE_KEY, [])


def related_cases_from_parents(node):
    """Get the cached related case ids from the parents of this node from

    1. The sysan of any parent nodes
    2. If any parents are cases, include those ids

    :param node: The Node instance
    :returns: List of str ids

    """

    # Get the cached ids from parents
    cases = {
        case_id
        for edge in node.edges_out
        if edge.dst
        for case_id in edge.dst._related_cases_from_cache
    }

    # Are any parents cases?
    for edge in node.edges_out:
        dst_class = Node.get_subclass_named(edge.__dst_class__)
        if dst_class.label == 'case' and edge.dst:
            cases.add(edge.dst.node_id)

    return list(cases)


def cache_related_cases_recursive(node,
                                  session,
                                  flush_context,
                                  instances,
                                  visited_nodes=None):
    """Update the related case cache on source node and its children
    recursively iff the this update changes the related case source
    node's cache.

    :param session: The target Session.
    :param flush_context:
        Internal UOWTransaction object which handles the details of
        the flush.
    :param instances:
        Usually None, this is the collection of objects which can be
        passed to the Session.flush() method (note this usage is
        deprecated).

    """

    if not node:
        return

    if visited_nodes and node.node_id in visited_nodes:
        return

    visited_nodes = (visited_nodes or set()).union({node.node_id})

    current_cases = related_cases_from_cache(node)
    updated_cases = related_cases_from_parents(node)
    diff = set(current_cases).symmetric_difference(updated_cases)

    if not diff:
        return

    node.sysan[CASE_CACHE_KEY] = updated_cases
    to_recurse = [e for e in node.edges_in if e.src]
    for edge in to_recurse:
        cache_related_cases_recursive(
            get_edge_src(edge),
            session,
            flush_context,
            instances,
            visited_nodes,
        )


def cache_related_cases_on_insert(target,
                                  session,
                                  flush_context,
                                  instances,
                                  visited_nodes=None):
    """Hook on updated edges.  Update the related case cache on source
    node and its children iff the this update changes the related case
    source node's cache.

    This will be called when a node association proxy instance is
    removed.

    :param session: The target Session.
    :param flush_context:
        Internal UOWTransaction object which handles the details of
        the flush.
    :param instances:
        Usually None, this is the collection of objects which can be
        passed to the Session.flush() method (note this usage is
        deprecated).

    """

    if not target.src:
        target.src = get_edge_src(target)

    if not target.dst:
        target.dst = get_edge_dst(target, allow_query=True)

    cache_related_cases_recursive(
        get_edge_src(target),
        session,
        flush_context,
        instances,
        visited_nodes,
    )


def cache_related_cases_on_update(target,
                                  session,
                                  flush_context,
                                  instances,
                                  visited_nodes=None):
    """Hook on deleted edges.  Update the related case cache on source
    node and its children.

    This will be called when an edge instance is updated
    (i.e. association proxy is deleted).

    :param session: The target Session.
    :param flush_context:
        Internal UOWTransaction object which handles the details of
        the flush.
    :param instances:
        Usually None, this is the collection of objects which can be
        passed to the Session.flush() method (note this usage is
        deprecated).

    """

    cache_related_cases_recursive(
        get_edge_src(target),
        session,
        flush_context,
        instances,
        visited_nodes,
    )


def cache_related_cases_on_delete(target,
                                  session,
                                  flush_context,
                                  instances,
                                  visited_nodes=None):
    """Hook on deleted edges.  Update the related case cache on source
    node and its children.

    This will be called when an edge instance is explicitly deleted in
    the session.

    :param session: The target Session.
    :param flush_context:
        Internal UOWTransaction object which handles the details of
        the flush.
    :param instances:
        Usually None, this is the collection of objects which can be
        passed to the Session.flush() method (note this usage is
        deprecated).

    """

    # Remove the source and destination of application local
    # association_proxy so cache_related_cases_update_children doesn't
    # traverse the edge
    target.dst, target.src = None, None
    cache_related_cases_recursive(
        get_edge_src(target),
        session,
        flush_context,
        instances,
        visited_nodes,
    )
