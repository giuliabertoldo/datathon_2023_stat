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

# Specialization
specialization = pd.read_parquet('https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Specialization.parquet.gzip')

graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Specialization.csv" AS csvLine
MERGE (s:Specialization {
    id: toInteger(csvLine.id), 
    name: csvLine.name,
    description: csvLine.description
    })
"""
)

graph.run('CREATE INDEX specialization FOR (n:Specialization) ON (n.id)')

# Academy
academy = pd.read_parquet('https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Academy.parquet.gzip')

graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Academy.csv" AS csvLine
MERGE (a:Academy {
    id: toInteger(csvLine.id), 
    name: csvLine.name,
    description: csvLine.description
    })
"""
)

graph.run('CREATE INDEX academy FOR (n:Academy) ON (n.id)')

# Medium
medium = pd.read_parquet('https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Medium.parquet.gzip')

graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Medium.csv" AS csvLine
MERGE (m:Medium {
    id: toInteger(csvLine.id), 
    name: csvLine.name,
    description: csvLine.description
    })
"""
)

graph.run('CREATE INDEX medium FOR (n:Medium) ON (n.id)')

# Places
places = pd.read_parquet('https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Places.parquet.gzip')

graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Places.csv" AS csvLine
MERGE (m:Place {
    id: toInteger(csvLine.id), 
    name: csvLine.name
    })
"""
)

graph.run('CREATE INDEX place FOR (n:Place) ON (n.id)')

# Relations 
graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Places.csv" AS csvLine
MATCH (p1:Place {id: toInteger(csvLine.id)}), (p2:Place {id: toInteger(csvLine.parent)})
MERGE (p1) -[r:LOCATED_IN]-> (p2)
"""
)

# Artist pictures
pictures = pd.read_parquet('https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/ArtistPicture.parquet.gzip')

graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/ArtistPicture.csv" AS csvLine
CREATE (m:Picture {
    id: toInteger(csvLine.id), 
    url: csvLine.url,
    source_url: csvLine.source_url,
    caption: csvLine.caption
    })
"""
)

graph.run('CREATE INDEX picture FOR (n:Picture) ON (n.id)')

# Generated artworks
graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Generated.csv" AS csvLine
CREATE (m:Generated {
    url: csvLine.url
    })
"""
)

# Artwork 
artworks = pd.read_parquet('https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Artwork.parquet.gzip')

graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Artwork.csv" AS csvLine
CREATE (m:Artwork {
    id: toInteger(csvLine.id), 
    name: csvLine.name,
    image_url: csvLine.image_url,
    rating: toInteger(csvLine.rating),
    summary: csvLine.summary,
    year: toIntegerOrNull(csvLine.year),
    location: csvLine.location
    })
"""
)

graph.run('CREATE INDEX artwork FOR (n:Artwork) ON (n.id)')

# Relation to medium 
graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Artwork.csv" AS csvLine
MATCH (a:Artwork {id: toInteger(csvLine.id)}), (m:Medium {id: toIntegerOrNull(csvLine.medium)})
MERGE (a) -[r:USES]-> (m)
"""
)

# Relation with generated
generated = pd.read_parquet('https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Generated.parquet.gzip')

graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Generated.csv" AS csvLine
MATCH (a:Artwork {id: toInteger(csvLine.source_artwork)}), (g:Generated {url: csvLine.url})
MERGE (g) -[r:BASED_ON]-> (a)
"""
)

# Recommendations
graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Recommendation.csv" AS csvLine
MATCH (a:Artwork {id: toInteger(csvLine.artwork)}), (recommendation:Artwork {id: toInteger(csvLine.recommended)})
MERGE (a) -[r:RECOMMENDS]-> (recommendation)
"""
)

# Artists
artists = pd.read_parquet('https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Artist.parquet.gzip')

graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Artist.csv" AS csvLine
CREATE (m:Artist {
    id: toInteger(csvLine.id), 
    name: csvLine.name,
    url: csvLine.url,
    summary: csvLine.summary
    })
"""
)

graph.run('CREATE INDEX artist FOR (n:Artist) ON (n.id)')

# Relationship to picture 
graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Artist.csv" AS csvLine
MATCH (a:Artist {id: toInteger(csvLine.id)}), (picture:Picture {id: toIntegerOrNull(csvLine.picture)})
MERGE (a) -[r:IMAGE]-> (picture)
"""
)

# Relationship to birth and death place 
graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Artist.csv" AS csvLine
MATCH (a:Artist {id: toInteger(csvLine.id)}), (birthplace:Place {id: toIntegerOrNull(csvLine.birthplace)})
MERGE (a) -[r:BORN_IN]-> (birthplace)
"""
)

graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Artist.csv" AS csvLine
MATCH (a:Artist {id: toInteger(csvLine.id)}), (deathplace:Place {id: toIntegerOrNull(csvLine.deathplace)})
MERGE (a) -[r:DIED_IN]-> (deathplace)
SET r.cause = csvLine.cause_of_death
"""
)

# Apprenticeship 
graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Apprenticeship.csv" AS csvLine
MATCH (student:Artist {id: toInteger(csvLine.student_id)}), (teacher:Artist {id: toIntegerOrNull(csvLine.teacher_id)})
MERGE (student) -[r:APPRENTICE_OF]-> (teacher)
"""
)

# Relations

## Art - Artwork 
graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/Artwork.csv" AS csvLine
MATCH (artwork:Artwork {id: toInteger(csvLine.id)}), (artist:Artist {id: toIntegerOrNull(csvLine.artist)})
MERGE (artwork) -[r:MADE_BY]-> (artist)
"""
)

## Artist - Specialization/Movement/Education
graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/ArtistSpecializations.csv" AS csvLine
MATCH (s:Specialization {id: toInteger(csvLine.specialty_id)}), (artist:Artist {id: toIntegerOrNull(csvLine.artist_id)})
MERGE (s) <-[r:SPECIALIZED_IN]- (artist)
"""
)

graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/ArtistMovements.csv" AS csvLine
MATCH (s:Movement {id: toInteger(csvLine.movement_id)}), (artist:Artist {id: toIntegerOrNull(csvLine.artist_id)})
MERGE (s) <-[r:BELONGS_TO]- (artist)
"""
)

graph.run(
"""
LOAD CSV WITH HEADERS FROM "https://kuleuven-datathon-2023.s3.eu-central-1.amazonaws.com/data/ArtistEducation.csv" AS csvLine
MATCH (s:Academy {id: toInteger(csvLine.academy_id)}), (artist:Artist {id: toIntegerOrNull(csvLine.artist_id)})
MERGE (s) <-[r:EDUCATED_AT]- (artist)
"""
)


