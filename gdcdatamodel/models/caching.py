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
from dictionaryutils import dictionary as gdcdictionary

logger = get_logger('gdcdatamodel')

CACHE_CASES = (
    True if (
        not hasattr(gdcdictionary, 'settings')
        or not gdcdictionary.settings)
    else gdcdictionary.settings.get('enable_case_cache', True)
)

#: This variable contains the link name for the case shortcut
#: association proxy
RELATED_CASES_LINK_NAME = '_related_cases'

#: This variable specifies the categories for which we won't create
#short cut : edges to case
NOT_RELATED_CASES_CATEGORIES = {
    'administrative',
    'TBD',
}


def get_related_case_edge_cls(node):
    """Returns the Edge class for related cases of a given node

    :param node: The source node (or type(node)) of the edge
    :returns: Edge subclass

    """

    return next(
        edge
        for edge in Edge.__subclasses__()
        if edge.__name__ == get_related_case_edge_cls_name(node)
    )


def get_related_case_edge_cls_name(node):
    """Standard generation of shortcut edge class name

    :param node: The source node (or type(node)) of the edge
    :returns: String name

    """

    return '{}RelatesToCase'.format(node.__class__.__name__)


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
    """Get the cached related case ids from this node's case shortcut
    edges

    :param node: The Node instance
    :returns: List of Case nodes

    """

    return filter(None, getattr(node, RELATED_CASES_LINK_NAME, []))


def related_cases_from_parents(node):
    """Get the cached related case ids from the parents of this node from

    1. The shortcut edges of any parent nodes
    2. If any parents are cases, include those ids

    :param node: The Node instance
    :returns: List of Case nodes

    """

    skip_edges_named = [
        get_related_case_edge_cls_name(node)
    ]

    # Get the cached ids from parents
    cases = {
        case
        for edge in node.edges_out
        if edge.dst
        for case in edge.dst._related_cases_from_cache
        if edge.__class__.__name__ not in skip_edges_named
    }

    # Are any parents cases?
    for edge in node.edges_out:
        if edge.__class__.__name__ in skip_edges_named:
            continue
        dst_class = Node.get_subclass_named(edge.__dst_class__)
        if dst_class.label == 'case' and edge.dst:
            cases.add(edge.dst)

    return filter(None, cases)


def cache_related_cases_recursive(node,
                                  session,
                                  flush_context,
                                  instances,
                                  visited_nodes=None):
    """Update the related case cache on source node and its children
    recursively iff the this update changes the related case source
    node's shortcut edges.

    :param session: The target Session.
    :param flush_context:
        Internal UOWTransaction object which handles the details of
        the flush.
    :param instances:
        Usually None, this is the collection of objects which can be
        passed to the Session.flush() method (note this usage is
        deprecated).

    """

    visited_nodes = set() if visited_nodes is None else visited_nodes

    # Check preconditions for updating shortcut edge
    if not node:
        return

    if not hasattr(node, RELATED_CASES_LINK_NAME):
        return

    if visited_nodes and node.node_id in visited_nodes:
        return

    visited_nodes.add(node.node_id)

    # These are the cases that are currently connected by a shortcut edge
    current_cases = {c.node_id: c for c in related_cases_from_cache(node)}

    # These are the cases are currently connected by a shortcut edge
    # to this node's parents
    updated_cases = {c.node_id: c for c in related_cases_from_parents(node)}

    current_case_ids = set(current_cases.keys())
    updated_case_ids = set(updated_cases.keys())
    diff = current_case_ids.symmetric_difference(updated_case_ids)

    # If nothing has changed, we don't need to update or recur
    if not diff:
        return

    update_cache_edges(node, session, updated_cases)

    to_recur = [e for e in node.edges_in if e.src]
    for edge in to_recur:
        cache_related_cases_recursive(
            get_edge_src(edge),
            session,
            flush_context,
            instances,
            visited_nodes,
        )

    return


def update_cache_edges(node, session, correct_cases):
    """Creates new edges or deletes old edges"""

    assoc_proxy = getattr(node, RELATED_CASES_LINK_NAME)

    # Get information about the existing edges
    edge_name = get_related_case_edge_cls_name(node)
    existing_edges = getattr(node, '_{}_out'.format(edge_name))

    # Remove edges that should no longer exist
    cases_disconnected = [
        edge.dst
        for edge in existing_edges
        if edge.dst_id not in correct_cases
    ]

    for case in cases_disconnected:
        assoc_proxy.remove(case)

    existing_edge_dst_case_ids = {
        edge.dst_id for edge in existing_edges
    }

    cases_connected = [
        case
        for case_id, case in correct_cases.iteritems()
        if case_id not in existing_edge_dst_case_ids
    ]

    for case in cases_connected:
        assoc_proxy.append(case)


def cache_related_cases_on_insert(target,
                                  session,
                                  flush_context,
                                  instances):
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
    )


def cache_related_cases_on_update(target,
                                  session,
                                  flush_context,
                                  instances):
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
    )


def cache_related_cases_on_delete(target,
                                  session,
                                  flush_context,
                                  instances):
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
    )
