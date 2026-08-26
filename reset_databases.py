"""
Wipes all 5 platforms to a clean state, so each ingestion_stability.py
iteration starts from zero rather than compounding on top of the previous
run's data (which would corrupt both correctness and timing).
"""

import os
import sys
import redis
from neo4j import GraphDatabase
from arango import ArangoClient
from dotenv import load_dotenv

load_dotenv()


def batched_delete(session, batch_size=2000, max_iterations=200, progress_interval=10):
    """
    Deletes all nodes/relationships in small batches instead of one
    unbounded transaction. A single MATCH (n) DETACH DELETE n holds the
    entire delete set in memory before committing — on Neo4j's tightly
    capped 128MB heap, deleting all 10,371 nodes / 122,886 relationships
    at once caused a "Java heap space" OOM. Batching keeps each
    transaction small enough to fit the resource cap, mirroring the same
    approach already used for ingestion.

    Added progress logging so long runs show activity instead of appearing
    to hang silently.
    """
    for i in range(max_iterations):
        session.run("MATCH (n) WITH n LIMIT $batch_size DETACH DELETE n", batch_size=batch_size)
        remaining = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
        if i % progress_interval == 0:
            print(f"  batched_delete: iteration {i+1}, remaining nodes: {remaining}")
        if remaining == 0:
            return
    raise RuntimeError(f"Batched delete did not finish within {max_iterations} iterations")


def reset_cypher_db(name, uri, user, password):
    if not uri:
        print(f"[{name}] Skipped: URI not configured.")
        return
    try:
        auth = (user, password) if user or password else None
        print(f"[{name}] Connecting to {uri} (will timeout after 10s if unreachable)...")
        # Add a short connection timeout to avoid very long hangs
        driver = GraphDatabase.driver(uri, auth=auth, connection_timeout=10)
        with driver.session() as session:
            print(f"[{name}] Connected — starting batched delete")
            batched_delete(session)
        driver.close()
        print(f"[{name}] Wiped clean.")
    except Exception as e:
        print(f"[{name}] Reset failed: {e}")
        sys.exit(1)


def reset_falkordb(host, port):
    try:
        # Use short socket connect timeout so an unreachable Redis doesn't hang
        r = redis.Redis(host=host, port=int(port), socket_connect_timeout=5, socket_timeout=5)
        try:
            r.execute_command("GRAPH.DELETE", "movielens")
        except Exception:
            pass  # graph may not exist yet on first run — not an error
        print("[Local FalkorDB] Wiped clean.")
    except Exception as e:
        print(f"[Local FalkorDB] Reset failed: {e}")
        sys.exit(1)


def reset_arangodb(url, user, password):
    try:
        client = ArangoClient(hosts=url)
        # Safely read system credentials; python-arango accepts None for missing creds
        sys_user = user if user else None
        sys_pass = password if password else None
        sys_db = client.db("_system", username=sys_user, password=sys_pass)
        db_name = "movielens_db"
        if sys_db.has_database(db_name):
            sys_db.delete_database(db_name)
        print("[Local ArangoDB] Wiped clean.")
    except Exception as e:
        print(f"[Local ArangoDB] Reset failed: {e}")
        sys.exit(1)


def main():
    print("Resetting all platforms to a clean state...")

    reset_cypher_db("CognoDB Cloud", os.getenv("COGNODB_URI"), os.getenv("COGNODB_USER"), os.getenv("COGNODB_PASSWORD"))
    reset_cypher_db("Local Neo4j", os.getenv("NEO4J_URI"), os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
    reset_cypher_db("Local Memgraph", os.getenv("MEMGRAPH_URI"), os.getenv("MEMGRAPH_USER") or "", os.getenv("MEMGRAPH_PASSWORD") or "")
    reset_falkordb(os.getenv("FALKORDB_HOST", "localhost"), os.getenv("FALKORDB_PORT", 6379))
    reset_arangodb(os.getenv("ARANGODB_URL", "http://localhost:8529"), os.getenv("ARANGODB_USER", "root"), os.getenv("ARANGODB_PASSWORD", "password123"))

    print("Reset complete.")


if __name__ == "__main__":
    main()
