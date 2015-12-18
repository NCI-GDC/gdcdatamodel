from psqlgraph import Node, Edge

traversals = {}
terminal_nodes = ['annotations', 'centers', 'archives', 'tissue_source_sites',
                  'files', 'related_files', 'describing_files']


def construct_traversals(root, node, visited, path):
    recurse = lambda neighbor: (
        neighbor
        # no backtracking
        and neighbor not in visited
        and neighbor != node
        # no traveling THROUGH terminal nodes
        and (path[-1] not in terminal_nodes
             if path else neighbor.label not in terminal_nodes))

    for edge in Edge._get_edges_with_src(node.__name__):
        neighbor = [n for n in Node.get_subclasses()
                    if n.__name__ == edge.__dst_class__][0]
        if recurse(neighbor):
            construct_traversals(
                root, neighbor, visited+[node], path+[edge.__src_dst_assoc__])

    for edge in Edge._get_edges_with_dst(node.__name__):
        neighbor = [n for n in Node.get_subclasses()
                    if n.__name__ == edge.__src_class__][0]
        if recurse(neighbor):
            construct_traversals(
                root, neighbor, visited+[node], path+[edge.__dst_src_assoc__])

    traversals[root][node.label] = traversals[root].get(node.label) or set()
    traversals[root][node.label].add('.'.join(path))


for node in Node.get_subclasses():
    traversals[node.label] = {}
    construct_traversals(node.label, node, [node], [])


def union_subq_without_path(q, *args, **kwargs):
    return q.except_(union_subq_path(q, *args, **kwargs))


def union_subq_path(q, dst_label, post_filters=[]):
    src_label = q.entity().label
    if not traversals.get(src_label, {}).get(dst_label, {}):
        return q
    paths = list(traversals[src_label][dst_label])
    base = q.subq_path(paths.pop(), post_filters)
    while paths:
        base = base.union(q.subq_path(paths.pop(), post_filters))
    return base
