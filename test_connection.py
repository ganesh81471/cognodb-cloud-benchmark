import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment variables from .env file
load_dotenv()

uri = os.getenv("COGNODB_URI")
user = os.getenv("COGNODB_USER")
password = os.getenv("COGNODB_PASSWORD")

def test_cognodb():
    print("Connecting to CognoDB Cloud...")
    try:
        # Create Neo4j driver connection
        driver = GraphDatabase.driver(uri, auth=(user, password))
        
        # Run a simple test query
        with driver.session() as session:
            result = session.run("RETURN 'Hello CognoDB!' AS message")
            record = result.single()
            print("SUCCESS! Response from database:", record["message"])
            
        driver.close()
    except Exception as e:
        print("ERROR: Connection failed!")
        print(e)

if __name__ == "__main__":
    test_cognodb()