from neo4j import GraphDatabase, basic_auth
import pandas as pd 

# Neo4j 
driver = GraphDatabase.driver(
  "bolt://54.161.134.53:7687",
  auth=basic_auth("neo4j", "loaf-acronyms-rifling"))

cypher_query = '''
call db.schema.visualization
'''

with driver.session(database="neo4j") as session:
  results = session.read_transaction(
    lambda tx: tx.run(cypher_query,
                      limit="10").data())
  for record in results:
    print(record['count'])

driver.close()

# ARTISTS 

# How many Artists? 616
cypher_query = '''
MATCH (n:Artist)
RETURN count(*)
'''

# How many relationships between artistics? 63 
cypher_query = '''
MATCH (n:Artist)-[r]->(m:Artist)
RETURN count(r)
'''

# Algorithm: Harmonic centrality - Artists 
cypher_query = '''
CALL gds.graph.project('myGraph', 'Artist', 'APPRENTICE_OF')
'''
cypher_query = '''
CALL gds.alpha.closeness.harmonic.stream('myGraph', {})
YIELD nodeId, centrality
RETURN gds.util.asNode(nodeId).name AS user, centrality
ORDER BY centrality DESC
'''



# ARTWORKS
## How many recommends unidirectional? 5774
cypher_query = '''
MATCH (n:Artwork)-[r:RECOMMENDS]->(m)
RETURN count(r)
'''

## How many artworks? 10517 
cypher_query = '''
MATCH (n:Artwork)
RETURN count(n)
'''

## Algorithm: Harmonic centrality - Artists 
cypher_query = '''
CALL gds.graph.project('myGraphWork', 'Artwork', 'RECOMMENDS')
'''
cypher_query = '''
CALL gds.alpha.closeness.harmonic.stream('myGraphWork', {})
YIELD nodeId, centrality
RETURN gds.util.asNode(nodeId).name AS user, centrality
ORDER BY centrality DESC
'''
