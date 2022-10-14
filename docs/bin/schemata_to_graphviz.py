import os

from graphviz import Digraph

from gdcdatamodel import models as m


def build_visualization():
    print('Building schema documentation...')

    # Load directory tree info
    bin_dir = os.path.dirname(os.path.realpath(__file__))
    root_dir = os.path.join(os.path.abspath(
        os.path.join(bin_dir, os.pardir, os.pardir)))

    # Create graph
    dot = Digraph(
        comment="High level graph representation of GDC data model", format='pdf')
    dot.graph_attr['rankdir'] = 'RL'
    dot.node_attr['fillcolor'] = 'lightblue'
    dot.node_attr['style'] = 'filled'

    # Add nodes
    for node in m.Node.get_subclasses():
        label = node.get_label()
        print label
        dot.node(label, label)

    # Add edges
    for edge in m.Edge.get_subclasses():
        if edge.__dst_class__ == 'Case' and edge.label == 'relates_to':
            # Skip case cache edges
            continue

        src = m.Node.get_subclass_named(edge.__src_class__)
        dst = m.Node.get_subclass_named(edge.__dst_class__)
        dot.edge(src.get_label(), dst.get_label(), edge.get_label())

    gv_path = os.path.join(root_dir, 'docs', 'viz', 'gdc_data_model.gv')
    dot.render(gv_path)
    print('graphviz output to {}'.format(gv_path))


if __name__ == '__main__':
    build_visualization()
