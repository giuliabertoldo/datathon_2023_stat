# SOURCE: https://towardsdatascience.com/neo4j-cypher-python-7a919a372be7 
from pandas import DataFrame
from neo4j import GraphDatabase
import pandas as pd
import matplotlib.pyplot as plt

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
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/giuliabertoldo/datathon_2023_stat/main/artists.csv" AS csvLine
CREATE (m:Artist {
    id: toInteger(csvLine.id), 
    name: csvLine.name,
    last_name: csvLine.last_name,
    birthplace: csvLine.birthplace,
    deathplace: csvLine.deathplace,
    birthdate: csvLine.birthdate,
    deathdate: csvLine.deathdate,
    cause_of_death: csvLine.cause_of_death,
    url: csvLine.url,
    summary: csvLine.summary
    })
'''
conn.query(query_string, db='datathon')

# Add edges 
query_string = '''
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/giuliabertoldo/datathon_2023_stat/main/appr.csv" AS csvLine
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
CALL gds.graph.project.cypher(
  'graph1',
  'MATCH (n:Artist) RETURN id(n) AS id',
  'MATCH (n:Artist)<-[r:APPRENTICE_OF]-(m:Artist) RETURN id(n) AS source, id(m) AS target')
'''
conn.query(query_string, db='datathon')

# DEGREE CENTRALITY 
query_string = '''
CALL gds.degree.write('graph1', { writeProperty: 'degree' })
YIELD centralityDistribution, nodePropertiesWritten
RETURN centralityDistribution.min AS minimumScore, centralityDistribution.mean AS meanScore, nodePropertiesWritten
'''
conn.query(query_string, db='datathon')

# Create dataframe with name and degree score
query_string = '''
MATCH (p:Artist)
RETURN DISTINCT p.name, p.last_name, p.degree
'''

df_degree = DataFrame([dict(_) for _ in conn.query(query_string, db='datathon')])

df_degree_sorted = df_degree.sort_values(by=['p.degree'], ascending=False)
# Top 10 most higher degree artists
top_ten_degree  = df_degree_sorted[0:10]

top_twenty_degree = df_degree_sorted[0:20]

top_fifty_degree = df_degree_sorted[0:50]

# Count degrees
counts = df_degree.value_counts(subset=['p.degree'])
df_counts = pd.DataFrame(counts)
df_counts = df_counts.rename(columns={0: 'count'})
df_counts['degree'] = [0, 1, 2]

# Visualize
fig, ax = plt.subplots()

counts = [539, 35, 5]
degree = ['0', '1', '2']

ax.bar(degree, counts)

ax.set_ylabel('Number of artists')
ax.set_xlabel('Degree Centrality')

plt.show()

# BETWEENESS 
# Create graph projection
query_string = '''
CALL gds.graph.project.cypher(
  'graph2',
  'MATCH (n:Artist) RETURN id(n) AS id',
  'MATCH (n:Artist)-[r:APPRENTICE_OF]-(m:Artist) RETURN id(n) AS source, id(m) AS target')
'''
conn.query(query_string, db='datathon')

query_string = '''
CALL gds.betweenness.write('graph2', { writeProperty: 'betweenness' })
YIELD centralityDistribution, nodePropertiesWritten
RETURN centralityDistribution.min AS minimumScore, centralityDistribution.mean AS meanScore, nodePropertiesWritten

'''
conn.query(query_string, db='datathon')

# Create dataframe with name and between score
query_string = '''
MATCH (p:Artist)
RETURN DISTINCT p.name, p.betweenness
'''

df_betweenness = DataFrame([dict(_) for _ in conn.query(query_string, db='datathon')])

df_betweenness_sorted = df_betweenness.sort_values(by=['p.betweenness'], ascending=False)

# Top 10 most higher df_betweenness_sorted artists
top_ten_df_betweenness_sorted  = df_betweenness_sorted[0:10]




conn.close()