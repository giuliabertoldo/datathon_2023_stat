from neo4j import GraphDatabase, basic_auth
import pandas as pd 

# Neo4j 
driver = GraphDatabase.driver(
  "bolt://54.161.134.53:7687",
  auth=basic_auth("neo4j", "loaf-acronyms-rifling"))

# Return all artists
def get_artists(tx):
    result = tx.run("MATCH (p:Artist) RETURN p.name AS name")
    records = list(result)  # a list of Record objects
    summary = result.consume()
    return records, summary 

with driver.session(database="neo4j") as session:
    records, summary = session.execute_read(get_artists)

    # Summary information
    print("The query `{query}` returned {records_count} records in {time} ms.".format(
        query=summary.query, records_count=len(records),
        time=summary.result_available_after
    ))

    # Loop through results and do something with them
    for person in records:
        print(person.data())  # obtain record as dict

# Run Harmonic Centrality algorithm 
def harmonic(tx):
    result = tx.run("
    CALL gds.alpha.closeness.harmonic.stream('myGraph', {})
 YIELD nodeId, centrality
 RETURN gds.util.asNode(nodeId).name AS user, centrality
 ORDER BY centrality DESC")
    records = list(result)  # a list of Record objects
    summary = result.consume()
    return records, summary 

with driver.session(database="neo4j") as session:
    records, summary = session.execute_read(get_artists)

    # Summary information
    print("The query `{query}` returned {records_count} records in {time} ms.".format(
        query=summary.query, records_count=len(records),
        time=summary.result_available_after
    ))

    # Loop through results and do something with them
    for person in records:
        print(person.data())  # obtain record as dict


driver.close()

