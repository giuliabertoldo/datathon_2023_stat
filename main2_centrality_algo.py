# SOURCE: https://towardsdatascience.com/neo4j-cypher-python-7a919a372be7 

from neo4j import GraphDatabase
class Neo4jConnection:
    
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
        
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, query, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = list(session.run(query))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response

# Create a connection instance
conn = Neo4jConnection(uri="bolt://54.161.134.53:7687", user="neo4j", pwd="loaf-acronyms-rifling")

# Create a database called datathon
conn.query("CREATE OR REPLACE DATABASE datathon")

# Add Artists nodes
query_string = '''
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Artist.csv" AS csvLine
CREATE (m:Artist {
    id: toInteger(csvLine.id), 
    name: csvLine.name,
    url: csvLine.url,
    summary: csvLine.summary
    })
'''
conn.query(query_string, db='datathon')

# Add edges 
query_string = '''
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Apprenticeship.csv" AS csvLine
MATCH (student:Artist {id: toInteger(csvLine.student_id)}), (teacher:Artist {id: toIntegerOrNull(csvLine.teacher_id)})
MERGE (student) -[r:APPRENTICE_OF]-> (teacher)
'''
conn.query(query_string, db='datathon')

# Get how many artists 
query_string = '''
MATCH (n:Artist)
RETURN count(*)
'''
conn.query(query_string, db='datathon')

# See first 25 artists
query_string = '''
MATCH (n:Artist)
RETURN n
LIMIT 25
'''
conn.query(query_string, db='datathon')

# Create graph projection
query_string = '''
CALL gds.graph.project(
  'graph1',            
  'Artist',             
  'APPRENTICE_OF'               
)
'''
conn.query(query_string, db='datathon')

# Page rank 
query_string = '''
CALL gds.pageRank.write('graph1', {
  writeProperty: 'pagerank'
})
YIELD nodePropertiesWritten, ranIterations
'''
conn.query(query_string, db='datathon')

# Betweenness 
query_string = '''
CALL gds.betweenness.write('graph1', { 
  writeProperty: 'betweenness' })
YIELD minimumScore, maximumScore, scoreSum, nodePropertiesWritten
'''
conn.query(query_string, db='datathon')


# Compute Harmonic Centrality Algorithm
query_string = '''
CALL gds.alpha.closeness.harmonic.stream('graph1', {})
YIELD nodeId, centrality
RETURN gds.util.asNode(nodeId).name AS user, centrality
ORDER BY centrality DESC
'''
conn.query(query_string, db='datathon')

query_string = '''
CALL gds.alpha.closeness.harmonic.write('graph1', {})
YIELD nodes, writeProperty
'''
conn.query(query_string, db='datathon')

# Import data in Pandas 
from pandas import DataFrame

# Create dataframe with name and centrality score
query_string = '''
MATCH (p:Artist)
RETURN DISTINCT p.name, p.centrality
'''
df_centrality = DataFrame([dict(_) for _ in conn.query(query_string, db='datathon')])
df_centrality.sample(10)

# Top 10 most central artists
df_centrality.sort_values(by=['p.centrality'], ascending=False)[0:10]

# 
conn.close()