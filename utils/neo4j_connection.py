from neo4j import GraphDatabase

def create_neo4j_driver(uri, user, password):
    """
    Create and return a Neo4j driver instance.
    
    Args:
        uri (str): The URI of the Neo4j database.
        user (str): The username to authenticate with.
        password (str): The password to authenticate with.

    Returns:
        GraphDatabase.driver: A Neo4j driver instance.
    """
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        print(f"Successfully connected to Neo4j graph {user}!")
        return driver
    except Exception as e:
        print(f"Failed to connect to Neo4j: {e}")
        driver.close()
        raise e
