#!/usr/bin/env python

from psqlgraph import Node, Edge
from gdcdatamodel import models as md


CACHE_EDGES = {
    Node.get_subclass_named(edge.__src_class__): edge
    for edge in Edge.get_subclasses()
    if "RelatesToCase" in edge.__name__
}


LEVEL_1_SQL = """

INSERT INTO {cache_edge_table} (src_id, dst_id, _props, _sysan, acl)
SELECT {cls_table}.node_id, node_case.node_id,
       '{{}}'::jsonb, '{{}}'::jsonb, '{{}}'::text[]
    FROM {cls_table}

    -- Step directly to case
    JOIN {cls_to_case_edge_table}
         ON {cls_table}.node_id = {cls_to_case_edge_table}.src_id
    JOIN node_case
         ON node_case.node_id = {cls_to_case_edge_table}.dst_id

    -- Append only, e.g. insert only those missing
    WHERE NOT EXISTS (
          SELECT 1 FROM {cache_edge_table}
          WHERE {cls_table}.node_id = {cache_edge_table}.src_id
          AND   node_case.node_id   = {cache_edge_table}.dst_id)
"""

APPEND_CACHE_FROM_PARENT_SQL = """

INSERT INTO {cache_edge_table} (src_id, dst_id, _props, _sysan, acl)
SELECT DISTINCT {cls_table}.node_id, node_case.node_id,
                '{{}}'::jsonb, '{{}}'::jsonb, '{{}}'::text[]
    FROM {cls_table}

    -- Step to parent
    JOIN {cls_to_parent_edge_table}
         ON      {cls_table}.node_id = {cls_to_parent_edge_table}.src_id
    JOIN {parent_table}
         ON   {parent_table}.node_id = {cls_to_parent_edge_table}.dst_id

    -- Step to parent's related cases
    JOIN {parent_cache_edge_table}
         ON   {parent_table}.node_id = {parent_cache_edge_table}.src_id
    JOIN node_case
         ON        node_case.node_id = {parent_cache_edge_table}.dst_id

    -- Append only, e.g. insert only those missing
    WHERE NOT EXISTS (
         SELECT  1 FROM {cache_edge_table}
          WHERE  {cls_table}.node_id = {cache_edge_table}.src_id
            AND  node_case.node_id   = {cache_edge_table}.dst_id)
"""


def max_distances_from_case():
    """Breadth first search for max depth every class is from case"""

    distances = {}

    to_visit = [(md.Case, -1)]
    while to_visit:
        cls, level = to_visit.pop(0)

        if cls not in distances:
            children = (link["src_type"] for _, link in cls._pg_backrefs.items())
            to_visit.extend((child, level + 1) for child in children)

        distances[cls] = max(distances.get(cls, level + 1), level)

    return distances


def get_levels():
    """Returns a map of levels -> [classes] where a level is the max
    distance a class is from a case

    """

    distances = max_distances_from_case()
    distinct_distances = set(distances.values())

    levels = {
        level: [cls for cls, distance in distances.items() if distance == level]
        for level in distinct_distances
    }

    return levels


def append_cache_from_parent(graph, child, parent):
    """Creates case cache edges from :param:`parent` that do not already
    exist for :param:`child`

    Add child cache edges for:
        {child -> parent -> case} / {child -> case}

    """

    description = child.label + " -> " + parent.label + " -> case"

    if parent not in CACHE_EDGES:
        print("skipping:", description, ": parent is not cached")

    elif child not in CACHE_EDGES:
        print("skipping:", description, ": child is not cached")

    elif child is parent:
        print("skipping:", description, ": cycle")

    else:
        print(description)

        for cls_to_parent_edge in get_edges_between(child, parent):
            statement = APPEND_CACHE_FROM_PARENT_SQL.format(
                cache_edge_table=CACHE_EDGES[child].__tablename__,
                cls_table=child.__tablename__,
                cls_to_parent_edge_table=cls_to_parent_edge.__tablename__,
                parent_table=parent.__tablename__,
                parent_cache_edge_table=CACHE_EDGES[parent].__tablename__,
            )
            graph.current_session().execute(statement)


def append_cache_from_parents(graph, cls):
    """Creates case cache edges that all parents have that do not already
    exist

    """

    parents = {link["dst_type"] for link in cls._pg_links.values()}

    for parent in parents:
        append_cache_from_parent(graph, cls, parent)


def get_edges_between(src, dst):
    """Returns all edges from src -> dst (directionality matters)"""

    return [
        edge
        for edge in Edge.get_subclasses()
        if edge.__src_class__ == src.__name__
        and edge.__dst_class__ == dst.__name__
        and edge not in CACHE_EDGES.values()
    ]


def seed_level_1(graph, cls):
    """Set the case cache for all nodes max 1 step from case"""

    for case_edge in get_edges_between(cls, md.Case):
        statement = LEVEL_1_SQL.format(
            cache_edge_table=CACHE_EDGES[cls].__tablename__,
            cls_table=cls.__tablename__,
            cls_to_case_edge_table=case_edge.__tablename__,
        )

        print("Seeding {} through {}".format(cls.label, case_edge.__name__))
        graph.current_session().execute(statement)


def update_case_cache_append_only(graph):
    """Server-side update case cache for all entities

    1) Seed direct relationships from level L1 (1 step from case)

    2) Visit all nodes in levels stepping out from case and for each
       entity in that level L, add the related case edges from all
       parents in level L-1 that do not already exist in level L

    """

    cls_levels = get_levels()

    for cls in Node.get_subclasses():
        seed_level_1(graph, cls)

    for level in sorted(cls_levels)[2:]:
        print("\n\nLevel:", level)
        for cls in cls_levels[level]:
            append_cache_from_parents(graph, cls)


def main():
    print(
        "No main() action defined, please manually call "
        "update_case_cache_append_only(graph)"
    )


if __name__ == "__main__":
    main()
