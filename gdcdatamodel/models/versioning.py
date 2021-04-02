import os
import uuid

from sqlalchemy import and_, event, select

UUID_NAMESPACE_SEED = os.getenv("UUID_NAMESPACE_SEED", "86bb916a-24c5-48e4-8a46-5ea73a379d47")
UUID_NAMESPACE = uuid.UUID(UUID_NAMESPACE_SEED, version=4)


def __generate_hash(seed, label):
    namespace = UUID_NAMESPACE
    name = "{}-{}".format(seed, label)
    return str(uuid.uuid5(namespace, name))


def compute_tag(node):
    """Computes unique tag for given node
    Args:
        node (models.Node): mode instance
    Returns:
        str: computed tag
    """
    keys = {node.node_id if p == "node_id" else node.props[p] for p in node.tag_properties}
    seeds = {p.dst.tag or compute_tag(p.dst) for p in node.edges_out}
    keys.update(seeds)
    return __generate_hash(keys, node.label)


def get_tagged_version(node_id, table, tag, conn):
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
        and_(table.c._sysan["tag"].astext == tag, table.c.node_id != node_id)
    )
    max_version = 0
    for r in conn.execute(query):
        max_version = max(r._sysan.get("version", 0), max_version)

        # reset latest
        r._sysan["latest"] = False
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
        tag = compute_tag(node)

        version = get_tagged_version(node.node_id, table, tag, conn)

        node._sysan["tag"] = tag
        node._sysan["latest"] = True
        node._sysan["version"] = version

        # update tag and version
        conn.execute(
            table.update()
            .where(table.c.node_id == node.node_id)
            .values(_sysan=node._sysan)
        )
