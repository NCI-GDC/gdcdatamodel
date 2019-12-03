"""
gdcdatamodel.migrations.update_case_cache
--------------------

Functionality to fix stale case caches in _related_cases edge tables.

"""

from gdcdatamodel.models import Case


def update_related_cases(driver, node_id):
    """Removes and re-adds the edge between the given node and it's parent
    to cascade the new relationship to _related_cases through the
    graph

    """

    with driver.session_scope() as session:

        node = driver.nodes().ids(node_id).one()
        edges_out = node.edges_out

        for edge in edges_out:
            edge_cls = edge.__class__
            copied_edge = edge_cls(
                src_id=edge.src_id,
                dst_id=edge.dst_id,
                properties=dict(edge.props),
                system_annotations=dict(edge.sysan),
            )

            # Delete the edge
            (
                driver.edges(edge_cls)
                .filter(edge_cls.src_id == copied_edge.src_id)
                .filter(edge_cls.dst_id == copied_edge.dst_id)
                .delete()
            )

            session.expunge(edge)

            # Re-add the edge to force a refresh of the stale cache
            session.add(copied_edge)

            # Assert the edge was re-added or abort the session
            (
                driver.edges(edge_cls)
                .filter(edge_cls.src_id == copied_edge.src_id)
                .filter(edge_cls.dst_id == copied_edge.dst_id)
                .one()
            )


def update_cache_cache_tree(driver, case):
    """Updates the _related_cases case cache for all children in the
    :param:`case` tree

    """

    visited = set()
    with driver.session_scope():
        for neighbor in case.edges_in:
            if neighbor.src_id in visited:
                continue
            update_related_cases(driver, neighbor.src_id)
            visited.add(neighbor.src_id)
