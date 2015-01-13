import json
import os
from gdcdatamodel import node_avsc_object, edge_avsc_object
from graphviz import Digraph

print('Building schema documentation...')

# Load directory tree info
bin_dir = os.path.dirname(os.path.realpath(__file__))
root_dir = os.path.join(os.path.abspath(os.path.join(bin_dir, os.pardir)))
schema_dir = os.path.join(bin_dir, 'gdcdatamodel', 'avro', 'schemata')

# Create graph
dot = Digraph(
    comment="High level graph representation of GDC data model", format='pdf')
dot.graph_attr['rankdir'] = 'RL'
dot.node_attr['fillcolor'] = 'lightblue'
dot.node_attr['style'] = 'filled'

# Load schema
node_schema = json.loads(str(node_avsc_object))
edge_schema = json.loads(str(edge_avsc_object))

# Add nodes
for node in node_schema:
    dot.node(node['name'], node['name'])
    print node['name']

# Add edges
for edge in edge_schema:
    for field in edge['fields']:
        if field['name'] == 'node_labels':
            for node_label in field['type']:
                src_label, dst_label = node_label['name'].split(':')
                dot.edge(src_label, dst_label, edge['name'])

gv_path = os.path.join(root_dir, 'docs', 'viz', 'gdc_data_model.gv')
dot.render(gv_path)
print('graphviz output to {}'.format(gv_path))
