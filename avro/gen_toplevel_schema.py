import json
import avro.schema


def load_file_list(file_list):
	known_schemata = avro.schema.Names()
	for filename in file_list:
		gen_schema = load_avsc(filename, known_schemata)

	return gen_schema

def load_avsc(file_path, names=None):
  	"""Load avsc file
 
       	args:
      	file_path: path to schema file
      	names(optional): avro.schema.Names object
  	"""
 
 	print("loading: %s" % file_path)
  	file_text = open(file_path).read()
  	json_data = json.loads(file_text)
 
  	schema = avro.schema.make_avsc_object(json_data, names)
 
  	return schema

def main():
	"""
	For now all hardcoded for initial prototype, should be a config
	"""

	node_schema_list = ["schemata/src/field_types.avsc", "schemata/src/node_types.avsc", "schemata/src/node_properties.avsc", "schemata/src/nodes.avsc"]
	node_schema = load_file_list(node_schema_list)
	output_file = open("schemata/gdc_nodes.avsc", "w")
	output_file.write(json.dumps(node_schema.to_json(), indent=2))
	output_file.close()

	edge_schema_list = ["schemata/src/node_types.avsc", "schemata/src/node_pairs.avsc", "schemata/src/edges.avsc"]
	edge_schema = load_file_list(edge_schema_list)	
	output_file = open("schemata/gdc_edges.avsc", "w")
	output_file.write(json.dumps(edge_schema.to_json(), indent=2))
	output_file.close()

	"""
	top_schema = node_schema.to_json() + edge_schema.to_json()
	output_file = open("schemata/gdc_nodes_edges.avsc", "w")
	output_file.write(json.dumps(top_schema, indent=2))
	output_file.close()
	"""

if __name__ == "__main__":
  	main()