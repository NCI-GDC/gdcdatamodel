from py2neo import neo4j
from graphviz import Digraph

# host = "172.16.128.74"
host = "localhost"

query_string = "MATCH (a)-[r]->(b) RETURN DISTINCT (a._type) AS This, type(r) as To, (b._type) AS That"
db = neo4j.GraphDatabaseService("http://{host}:7474/db/data/".format(host=host))
result = neo4j.CypherQuery(db, query_string).execute()

dot = Digraph(comment="XML Generate Datamodel")
index = 0
for r in result:
    print r.values
    this, label, that = r.values

    if not this: 
        this = str(index)
        index += 1
    if not that: 
        that = str(index)
        index += 1

    dot.node(this)
    dot.node(that)
    dot.edge(this, that, label=label)

dot.render('generated.gv')
