import json
from neo4j import GraphDatabase

def setup_database():
    # Load config
    with open("/Users/dhrumeen/projects/Text2Cypher/configs/config.json", 'r') as f:
        config = json.load(f)

    # Connect to Neo4j
    driver = GraphDatabase.driver(config["neo4j_uri"], auth=(config["neo4j_user"], config["neo4j_password"]))

    # Read the cypher script
    with open("/Users/dhrumeen/projects/Text2Cypher/neo4j_db/setup_db.cypher", 'r') as f:
        cypher_script = f.read()

    # Execute the script
    with driver.session() as session:
        for statement in cypher_script.split(';'):
            if statement.strip():
                session.run(statement)

    driver.close()

if __name__ == "__main__":
    setup_database()
