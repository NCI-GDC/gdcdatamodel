import models

from psqlgraph import Node, Edge

traversals = {}
terminal_nodes = ['annotation', 'center', 'archive', 'tissue_source_site']


def walk_graph(root, node, visited, path):

    for edge in Edge._get_edges_with_src(node.__name__):
        neighbor = [n for n in Node.get_subclasses()
                    if n.__name__ == edge.__dst_class__][0]
        if neighbor and neighbor not in visited\
           and neighbor != node and neighbor.label not in terminal_nodes:
            walk_graph(
                root, neighbor, visited+[node], path+[edge.__src_dst_assoc__])

    for edge in Edge._get_edges_with_dst(node.__name__):
        neighbor = [n for n in Node.get_subclasses()
                    if n.__name__ == edge.__src_class__][0]
        if neighbor and neighbor not in visited\
           and neighbor != node and neighbor.label not in terminal_nodes:
            walk_graph(
                root, neighbor, visited+[node], path+[edge.__dst_src_assoc__])

    traversals[root][node.label] = traversals[root].get(node.label) or set()
    traversals[root][node.label].add('.'.join(path))


for node in Node.get_subclasses():
    traversals[node.label] = {}
    walk_graph(node.label, node, [node], [])
