# NOTE: You should be logged in the Neo4j Sandbox for the code to work. If you get an error login at the bottom with username = neo4j and password the password below 

# Import packages
from py2neo import Graph
import pandas as pd
from neo4j import GraphDatabase


# Parameters
YOUR_PASSWORD: str = "loaf-acronyms-rifling"
YOUR_PORT: int = "bolt://54.161.134.53:7687"

# Connect to database 
graph = Graph(YOUR_PORT,password=YOUR_PASSWORD)
graph.run('match (n) detach delete n') # Drops all data
try:
    indexes = graph.run('show indexes yield name').to_data_frame()['name'] # drops all indices
    for index in indexes:
        graph.run(f'drop index {index}')
except:
    pass


# Artists
graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/giuliabertoldo/datathon_2023_stat/main/artists_clean.csv" AS csvLine
CREATE (m:Artist {
    id: toInteger(csvLine.id), 
    name: csvLine.name,
    url: csvLine.url,
    summary: csvLine.summary
    })
"""
)

graph.run('CREATE INDEX artist FOR (n:Artist) ON (n.id)')

graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Apprenticeship.csv" AS csvLine
MATCH (student:Artist {id: toInteger(csvLine.student_id)}), (teacher:Artist {id: toIntegerOrNull(csvLine.teacher_id)})
MERGE (student) -[r:APPRENTICE_OF]-> (teacher)
"""
)

