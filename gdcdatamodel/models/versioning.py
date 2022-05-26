import os
import uuid

import six
from sqlalchemy import and_, event, select
try:
    from functools import lru_cache
except ImportError:
    from functools32 import lru_cache


UUID_NAMESPACE_SEED = os.getenv(
    "UUID_NAMESPACE_SEED", "86bb916a-24c5-48e4-8a46-5ea73a379d47"
)
UUID_NAMESPACE = uuid.UUID("urn:uuid:{}".format(UUID_NAMESPACE_SEED), version=4)


class TagKeys:
    tag = "tag"
    latest = "latest"
    version = "ver"


class TaggingConstraint:
    """Computes whether a node instance supports tagging or not"""

    def __init__(self, path, prop, values):
        """
        Args:
            path (str): full psqlgraph path to a parent node
            prop (str): valid node property name
            values (list[str]): list of possible value
        """
        self.path = path
        self.prop = prop
        self.values = values

    def _resolve_target_node_from_path(self, node):
        """Resolves to the final node instance that can be used to perform the matching

        e.g: if path = `aligned_reads.submitted_alinged_reads`, the final node used to perform the matching
        is an instance of SubmittedAlignedReads, which can be reached by following the relationships defined in
        the path
            i.e: node["aligned_reads"][0]["submitted_aligned_reads"][0]

            this is equivalent to:
                node.aligned_reads[0].submitted_aligned_reads[0]

        Args:
            node (models.Node): Node instance

        Returns:
            models.Node: node instance whose properties will be used for matching
        """
        if not self.path:
            return node

        for path in self.path.split("."):
            # Since a node type can have multiple paths to a given parent
            # this check allows instances that do not have this specific path
            if len(node[path]) == 0:
                return None

            node = node[path][0]
        return node

    def match(self, node):
        """Checks if a node has a value matching the prop and values field

        if it does, the particular instance will not participate in the entire tagging process
        Args:
            node (psqlgraph.Node): node instance
        Returns:
            Returns (bool)
        """
        node = self._resolve_target_node_from_path(node)
        return node and node[self.prop] in self.values


class TaggingConfig:
    """A wrapper around the tagConfig definition in the dictionary yaml"""

    def __init__(self, cfg):
        """

        Args:
            cfg (dict[str, Any]): The tagConfig section of the dictionary
        """
        self.cfg = cfg

    def _constraints(self):
        """Returns all constraints defined for a particular node type"""

        skip_criterion = self.cfg.get("ignoreEntries", [])
        for criteria in skip_criterion:
            yield TaggingConstraint(
                path=criteria.get("path"),
                prop=criteria["prop"],
                values=criteria["values"],
            )

    def is_taggable(self, node):
        """Returns true if node supports tagging else False. Ideally, instances that return false will not
            have tag and version number set on them
        """
        return not any(criteria.match(node) for criteria in self._constraints())


def __generate_hash(seed, label):
    namespace = UUID_NAMESPACE
    name = "{}-{}".format(seed, label)
    return six.ensure_str(str(uuid.uuid5(namespace, name)))


@lru_cache(maxsize=None)
def compute_tag(node):
    """Computes unique tag for given node
    Args:
        node (models.Node): mode instance
    Returns:
        str: computed tag
    """
    keys = node.get_tag_property_values()
    keys += sorted(
        [
            six.ensure_str(compute_tag(p.dst))
            for p in node.edges_out
            if p.dst.is_taggable() and p.label != "relates_to"
        ]
    )
    return __generate_hash(keys, node.label)


def __get_tagged_version(node_id, table, tag, conn):
    """Super private function to figure out the proper version number to use just after insertion
    Args:
        node_id (str): current node_id
        table (sqlalchemy.Table): node table instance
        tag (str): currently computed tag
        conn (sqlalchemy.engine.Connection): currently active connection instance

    Returns:
        int: appropriate version number to use. 1 greater than the current max
    """
    query = select([table]).where(
        and_(table.c._sysan[TagKeys.tag].astext == tag, table.c.node_id != node_id)
    )
    max_version = 0
    for r in conn.execute(query):
        max_version = max(r._sysan.get(TagKeys.version, 0), max_version)

        # reset latest
        r._sysan[TagKeys.latest] = False
        conn.execute(
            table.update().where(table.c.node_id == r.node_id).values(_sysan=r._sysan)
        )
    return max_version + 1


def inject_set_tag_after_insert(cls):
    """Injects an event listener that sets the tag and version properties on nodes, just before they are inserted
    Args:
        cls (class): node class type
    """

    @event.listens_for(cls, "after_insert")
    def set_node_tag(mapper, conn, node):
        table = node.__table__

        if not node.is_taggable():
            return  # do nothing

        tag = compute_tag(node)

        version = __get_tagged_version(node.node_id, table, tag, conn)

        node._sysan[TagKeys.tag] = tag
        node._sysan[TagKeys.latest] = True
        node._sysan[TagKeys.version] = version

        # update tag and version
        conn.execute(
            table.update()
            .where(table.c.node_id == node.node_id)
            .values(_sysan=node._sysan)
        )
