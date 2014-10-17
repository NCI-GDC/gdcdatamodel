from py2neo import neo4j
from graphviz import Digraph

query_string = "MATCH (a)-[r]->(b) RETURN DISTINCT (a._type) AS This, type(r) as To, (b._type) AS That"
db = neo4j.GraphDatabaseService()
result = neo4j.CypherQuery(db, query_string).execute()

dot = Digraph(comment="XML Generate Datamodel")
for r in result:
    this, label, that = r.values
    dot.node(this)
    dot.node(that)
    dot.edge(this, that, label=label)

dot.render('generated.gv')
