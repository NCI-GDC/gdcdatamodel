import json
from graphviz import Digraph

dot = Digraph(comment="High level graph representation of GDC data model", format='pdf')
dot.graph_attr['rankdir'] = 'RL'
dot.node_attr['fillcolor'] = 'lightblue'
dot.node_attr['style'] = 'filled'

node_schema = json.loads(open("schemata/gdc_nodes.avsc").read())

for node in node_schema:
	dot.node(node['name'], node['name'])
	print node['name']


edge_schema = json.loads(open("schemata/gdc_edges.avsc").read())

for edge in edge_schema:
	for field in edge['fields']:
		if field['name'] == 'node_types':
			for node_type in field['type']:
				#print node_type
				src_dest_type = node_type['name'].split(':')
				dot.edge(src_dest_type[0], src_dest_type[1], edge['name'])
				#print ("node_type: %s" % node_type['name'])

dot.render('viz/gdc_data_model.gv')