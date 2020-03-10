from graphviz import Digraph


def create_graphviz(nodes, include_case_cache_edges=False):
    """Returns Graphviz Dot object.

    Call dot.render(path) to write to file
    """

    dot = Digraph()
    dot.graph_attr['rankdir'] = 'RL'

    edges_added = set()
    nodes = {node.node_id: node for node in nodes}

    def is_edge_drawn(edge, neighbor):
        is_case_cache_edge = 'RelatesToCase' in edge.__class__.__name__

        return (
            (include_case_cache_edges or not is_case_cache_edge) and
            edge not in edges_added and
            neighbor in nodes
        )

    for node in nodes.values():
        dot.node(node.node_id, str(node))

        for edge in node.edges_out:
            if is_edge_drawn(edge, edge.dst_id):
                dot.edge(edge.src_id, edge.dst_id)
                edges_added.add(edge)

        for edge in node.edges_in:
            if is_edge_drawn(edge, edge.src_id):
                dot.edge(edge.src_id, edge.dst_id)
                edges_added.add(edge)

    return dot
