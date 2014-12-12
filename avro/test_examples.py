import json
import avro.schema, avro.io

node_schema = avro.schema.parse(open("schemata/gdc_nodes.avsc").read())

valid_aliquot_data = open("examples/nodes/aliquot_valid.json").read()
valid_aliquot_json = json.loads(valid_aliquot_data)

print avro.io.validate(node_schema, valid_aliquot_json)

invalid_aliquot_data = open("examples/nodes/aliquot_invalid.json").read()
invalid_aliquot_json = json.loads(invalid_aliquot_data)

print avro.io.validate(node_schema, invalid_aliquot_json)