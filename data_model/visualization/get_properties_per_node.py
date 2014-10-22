from py2neo import neo4j
from pprint import pprint
import json

query_string = "MATCH (a) RETURN a"
db = neo4j.GraphDatabaseService()
results = neo4j.CypherQuery(db, query_string).execute()

mapping = {}

for result in results:
    node = result.values[0]
    _type = node['_type']

    if _type not in mapping: mapping[_type] = set()
    for key in node:
        mapping[_type].add(key)

mapping = {key: list(value) for key, value in mapping.iteritems()}

with open('node_properties.json', 'w') as fp:
    json.dump(mapping, fp)
