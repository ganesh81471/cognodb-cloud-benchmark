"""
Wipes all 5 platforms to a clean state, so each ingestion_stability.py
iteration starts from zero rather than compounding on top of the previous
run's data (which would corrupt both correctness and timing).

STRATEGY CHANGE (v3): Neo4j and Memgraph are now reset by destroying and
recreating their Docker containers, instead of running Cypher DELETE
queries against them.

Why: every Cypher-based delete strategy tried before this (unbounded
DETACH DELETE, small client-batched loops, server-managed
`CALL {} IN TRANSACTIONS`) eventually hit the same underlying wall — on a
128MB-heap-capped JVM, enough delete churn (whether one huge transaction
or hundreds of small ones) triggers a stop-the-world GC pause severe
enough that the server can't even respond to its own query timeout,
making the client hang indefinitely (unkillable by Ctrl+C, since it's
blocked in a low-level socket read). `CALL {} IN TRANSACTIONS` also
turned out to be unsupported syntax on this Neo4j edition/version
(confirmed via SyntaxError on iteration 1), so it wasn't even a viable
path in the first place.

Since none of the 4 local containers mount a persistent volume (confirmed
from the `docker run` commands used to create them — no `-v` flags), all
graph data lives in the container's ephemeral writable layer. That means
"wipe the database" and "destroy + recreate the container" are
equivalent, and the latter is a file-system-level operation with near-zero
JVM/heap involvement — it can't GC-thrash because there's no JVM doing
row-by-row delete work. Recreating from an already-pulled local image is
also fast (a few seconds), since no image re-download is needed.

CognoDB Cloud is a managed service — there's no local container to
recreate — so it keeps using the original Cypher-based batched delete
(relationships first, then nodes), which has worked reliably every run so
far because CognoDB's free-tier dataset here is small/empty by the time
this runs and hasn't shown the same GC-hang symptom.

ArangoDB and FalkorDB are unaffected by this change — their existing
resets (drop-database / GRAPH.DELETE) have been fast and reliable and
don't share Neo4j/Memgraph's JVM-heap-under-delete-churn failure mode.
"""

import os
import sys
import time
import subprocess
import redis
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable
from arango import ArangoClient
from dotenv import load_dotenv

load_dotenv()


# -----------------------------------------------------------------------------
# Docker container recreation (Neo4j, Memgraph)
# -----------------------------------------------------------------------------
# These mirror the exact `docker run` commands used to originally create
# each container (same --cpus/-m caps, same env vars, same port mappings),
# so resource parity with CognoDB's free tier is preserved on every reset.
DOCKER_CONTAINER_CONFIGS = {
    "Local Neo4j": {
        "container_name": "neo4j-bench",
        "run_args": [
            "-d", "--name", "neo4j-bench",
            "--cpus=0.5", "-m", "256m",
            "-p", "7474:7474", "-p", "7687:7687",
            "-e", "NEO4J_AUTH=neo4j/password123",
            "-e", "NEO4J_server_memory_heap_initial__size=128m",
            "-e", "NEO4J_server_memory_heap_max__size=128m",
            "-e", "NEO4J_server_memory_pagecache_size=64m",
            "neo4j:latest",
        ],
        "readiness_timeout_sec": 90,  # JVM boot is the slow part
        # BUG FIX: the container is created with NEO4J_AUTH=neo4j/password123,
        # so it REQUIRES login. The readiness check originally used
        # auth=None, which the driver can't even encode as a valid auth
        # token ("missing key `scheme`") — it's not "no auth needed", it's
        # "no auth token at all". Must match NEO4J_AUTH above exactly.
        "auth": ("neo4j", "password123"),
    },
    "Local Memgraph": {
        "container_name": "memgraph-bench",
        "run_args": [
            "-d", "--name", "memgraph-bench",
            "--cpus=0.5", "-m", "256m",
            "-p", "7688:7687",
            "memgraph/memgraph:latest", "--memory-limit=180",
        ],
        "readiness_timeout_sec": 30,  # in-memory engine, boots fast
        # Memgraph's default docker image has no auth configured (no
        # --auth flags passed in the run command), so auth=None is
        # actually correct here — unlike Neo4j above.
        "auth": None,
    },
}


def recreate_container(platform_name, uri):
    config = DOCKER_CONTAINER_CONFIGS[platform_name]
    container_name = config["container_name"]

    print(f"[{platform_name}] Removing container '{container_name}' (if it exists)...")
    # -f: force-remove even if running; ignore failure if container doesn't exist
    subprocess.run(["docker", "rm", "-f", container_name],
                    capture_output=True, text=True)

    print(f"[{platform_name}] Recreating container '{container_name}' "
          f"(same resource caps as original setup: --cpus=0.5 -m 256m)...")
    result = subprocess.run(["docker", "run"] + config["run_args"],
                             capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"'docker run' failed for {platform_name}: {result.stderr.strip()}"
        )

    print(f"[{platform_name}] Waiting for Bolt connectivity at {uri} "
          f"(up to {config['readiness_timeout_sec']}s)...")
    deadline = time.time() + config["readiness_timeout_sec"]
    last_error = None
    while time.time() < deadline:
        try:
            driver = GraphDatabase.driver(uri, auth=config["auth"], connection_timeout=5)
            driver.verify_connectivity()
            driver.close()
            print(f"[{platform_name}] Ready.")
            return
        except Exception as e:
            last_error = e
            time.sleep(2)

    raise RuntimeError(
        f"{platform_name} did not become ready within "
        f"{config['readiness_timeout_sec']}s. Last error: {last_error}"
    )


# -----------------------------------------------------------------------------
# CognoDB Cloud: still Cypher-based delete (no local container to recreate)
# -----------------------------------------------------------------------------
def batched_delete_cognodb(session, batch_size=300, max_iterations=2000,
                            progress_interval=5, query_timeout=20):
    def run_with_timeout(query, **params):
        try:
            return session.run(query, timeout=query_timeout, **params)
        except (Neo4jError, ServiceUnavailable) as e:
            raise RuntimeError(f"Query exceeded {query_timeout}s timeout or failed: {e}") from e

    print("  batched_delete: phase 1 — deleting relationships...")
    for i in range(max_iterations):
        result = run_with_timeout(
            "MATCH ()-[r]-() WITH r LIMIT $batch_size DELETE r RETURN count(r) AS deleted",
            batch_size=batch_size,
        )
        deleted = result.single()["deleted"]
        if i % progress_interval == 0:
            print(f"    rel batch {i + 1}: deleted {deleted}")
        if deleted == 0:
            break
    else:
        raise RuntimeError(f"Relationship deletion did not finish within {max_iterations} iterations")

    print("  batched_delete: phase 2 — deleting nodes...")
    for i in range(max_iterations):
        result = run_with_timeout(
            "MATCH (n) WITH n LIMIT $batch_size DELETE n RETURN count(n) AS deleted",
            batch_size=batch_size,
        )
        deleted = result.single()["deleted"]
        if i % progress_interval == 0:
            print(f"    node batch {i + 1}: deleted {deleted}")
        if deleted == 0:
            return
    raise RuntimeError(f"Node deletion did not finish within {max_iterations} iterations")


def reset_cognodb(uri, user, password):
    if not uri:
        print("[CognoDB Cloud] Skipped: URI not configured.")
        return
    try:
        auth = (user, password) if user or password else None
        print(f"[CognoDB Cloud] Connecting to {uri} (will timeout after 10s if unreachable)...")
        driver = GraphDatabase.driver(uri, auth=auth, connection_timeout=10)
        with driver.session() as session:
            print("[CognoDB Cloud] Connected — starting batched delete")
            batched_delete_cognodb(session)
        driver.close()
        print("[CognoDB Cloud] Wiped clean.")
    except Exception as e:
        print(f"[CognoDB Cloud] Reset failed: {e}")
        sys.exit(1)


# -----------------------------------------------------------------------------
# FalkorDB / ArangoDB: unchanged, already fast and reliable
# -----------------------------------------------------------------------------
def reset_falkordb(host, port):
    try:
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

    reset_cognodb(os.getenv("COGNODB_URI"), os.getenv("COGNODB_USER"), os.getenv("COGNODB_PASSWORD"))

    try:
        recreate_container("Local Neo4j", os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    except Exception as e:
        print(f"[Local Neo4j] Reset failed: {e}")
        sys.exit(1)

    try:
        recreate_container("Local Memgraph", os.getenv("MEMGRAPH_URI", "bolt://localhost:7688"))
    except Exception as e:
        print(f"[Local Memgraph] Reset failed: {e}")
        sys.exit(1)

    reset_falkordb(os.getenv("FALKORDB_HOST", "localhost"), os.getenv("FALKORDB_PORT", 6379))
    reset_arangodb(os.getenv("ARANGODB_URL", "http://localhost:8529"), os.getenv("ARANGODB_USER", "root"), os.getenv("ARANGODB_PASSWORD", "password123"))

    print("Reset complete.")


if __name__ == "__main__":
    main()