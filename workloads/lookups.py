import os
import json
import time
import random
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase
from arango import ArangoClient
import redis

load_dotenv()

DATA_DIR = os.path.join("dataset", "ml-latest-small")
RATINGS_PATH = os.path.join(DATA_DIR, "ratings.csv")
MOVIES_PATH = os.path.join(DATA_DIR, "movies.csv")
RESULTS_PATH = os.path.join("results", "lookup_benchmark_results.json")

WARMUP_RUNS = 10
MEASURED_RUNS = 100

ratings_df = pd.read_csv(RATINGS_PATH)
movies_df = pd.read_csv(MOVIES_PATH)

USER_IDS = ratings_df['userId'].unique().tolist()
MOVIE_IDS = movies_df['movieId'].unique().tolist()

def calc_stats(durations):
    return {
        "mean_ms": round(float(np.mean(durations)), 2),
        "p50_ms": round(float(np.percentile(durations, 50)), 2),
        "p95_ms": round(float(np.percentile(durations, 95)), 2),
        "min_ms": round(float(np.min(durations)), 2),
        "max_ms": round(float(np.max(durations)), 2)
    }

# -----------------------------------------------------------------------------
# 1. CYPHER LOOKUPS (CognoDB, Neo4j, Memgraph)
# -----------------------------------------------------------------------------
def run_cypher_lookups(platform, uri, user, password):
    if not uri:
        print(f"Skipping {platform}: URI missing.")
        return []

    auth = (user, password) if user or password else None
    driver = GraphDatabase.driver(uri, auth=auth)
    results = []

    with driver.session() as session:
        # --- A. Point Lookup (Fetch Single User by ID) ---
        # Warm-up
        for _ in range(WARMUP_RUNS):
            u_id = random.choice(USER_IDS)
            session.run("MATCH (u:User {userId: $id}) RETURN u", id=u_id)
        
        durations = []
        for _ in range(MEASURED_RUNS):
            u_id = random.choice(USER_IDS)
            start = time.perf_counter()
            res = session.run("MATCH (u:User {userId: $id}) RETURN u.userId", id=u_id)
            list(res)
            durations.append((time.perf_counter() - start) * 1000)

        stats = calc_stats(durations)
        print(f"[{platform}] Point Lookup -> p50: {stats['p50_ms']}ms | p95: {stats['p95_ms']}ms")
        results.append({"platform": platform, "workload": "Point_Lookup", **stats})

        # --- B. Indexed/Filtered Lookup (Fetch Movies by Title Filter) ---
        # Warm-up
        for _ in range(WARMUP_RUNS):
            session.run("MATCH (m:Movie) WHERE m.title CONTAINS 'Action' RETURN m LIMIT 20")

        durations = []
        for _ in range(MEASURED_RUNS):
            start = time.perf_counter()
            res = session.run("MATCH (m:Movie) WHERE m.title CONTAINS 'Matrix' RETURN m.movieId, m.title")
            list(res)
            durations.append((time.perf_counter() - start) * 1000)

        stats = calc_stats(durations)
        print(f"[{platform}] Filtered Lookup -> p50: {stats['p50_ms']}ms | p95: {stats['p95_ms']}ms")
        results.append({"platform": platform, "workload": "Filtered_Lookup", **stats})

    driver.close()
    return results

# -----------------------------------------------------------------------------
# 2. FALKORDB LOOKUPS
# -----------------------------------------------------------------------------
def run_falkordb_lookups(host, port):
    r = redis.Redis(host=host, port=int(port))
    graph_name = "movielens"
    results = []

    # --- A. Point Lookup ---
    for _ in range(WARMUP_RUNS):
        u_id = random.choice(USER_IDS)
        r.execute_command("GRAPH.QUERY", graph_name, f"MATCH (u:User {{userId: {u_id}}}) RETURN u")

    durations = []
    for _ in range(MEASURED_RUNS):
        u_id = random.choice(USER_IDS)
        start = time.perf_counter()
        r.execute_command("GRAPH.QUERY", graph_name, f"MATCH (u:User {{userId: {u_id}}}) RETURN u.userId")
        durations.append((time.perf_counter() - start) * 1000)

    stats = calc_stats(durations)
    print(f"[Local FalkorDB] Point Lookup -> p50: {stats['p50_ms']}ms | p95: {stats['p95_ms']}ms")
    results.append({"platform": "Local FalkorDB", "workload": "Point_Lookup", **stats})

    # --- B. Filtered Lookup ---
    for _ in range(WARMUP_RUNS):
        r.execute_command("GRAPH.QUERY", graph_name, "MATCH (m:Movie) WHERE m.title CONTAINS 'Action' RETURN m LIMIT 20")

    durations = []
    for _ in range(MEASURED_RUNS):
        start = time.perf_counter()
        r.execute_command("GRAPH.QUERY", graph_name, "MATCH (m:Movie) WHERE m.title CONTAINS 'Matrix' RETURN m.movieId, m.title")
        durations.append((time.perf_counter() - start) * 1000)

    stats = calc_stats(durations)
    print(f"[Local FalkorDB] Filtered Lookup -> p50: {stats['p50_ms']}ms | p95: {stats['p95_ms']}ms")
    results.append({"platform": "Local FalkorDB", "workload": "Filtered_Lookup", **stats})

    return results

# -----------------------------------------------------------------------------
# 3. ARANGODB LOOKUPS (AQL)
# -----------------------------------------------------------------------------
def run_arangodb_lookups(url, user, password):
    client = ArangoClient(hosts=url)
    db = client.db("movielens_db", username=user, password=password)
    results = []

    # --- A. Point Lookup ---
    for _ in range(WARMUP_RUNS):
        u_id = random.choice(USER_IDS)
        db.aql.execute(f"FOR u IN User FILTER u.userId == {u_id} RETURN u")

    durations = []
    for _ in range(MEASURED_RUNS):
        u_id = random.choice(USER_IDS)
        start = time.perf_counter()
        cursor = db.aql.execute(f"FOR u IN User FILTER u.userId == {u_id} RETURN u.userId")
        list(cursor)
        durations.append((time.perf_counter() - start) * 1000)

    stats = calc_stats(durations)
    print(f"[Local ArangoDB] Point Lookup -> p50: {stats['p50_ms']}ms | p95: {stats['p95_ms']}ms")
    results.append({"platform": "Local ArangoDB", "workload": "Point_Lookup", **stats})

    # --- B. Filtered Lookup ---
    for _ in range(WARMUP_RUNS):
        db.aql.execute("FOR m IN Movie FILTER CONTAINS(m.title, 'Action') LIMIT 20 RETURN m")

    durations = []
    for _ in range(MEASURED_RUNS):
        start = time.perf_counter()
        cursor = db.aql.execute("FOR m IN Movie FILTER CONTAINS(m.title, 'Matrix') RETURN {movieId: m.movieId, title: m.title}")
        list(cursor)
        durations.append((time.perf_counter() - start) * 1000)

    stats = calc_stats(durations)
    print(f"[Local ArangoDB] Filtered Lookup -> p50: {stats['p50_ms']}ms | p95: {stats['p95_ms']}ms")
    results.append({"platform": "Local ArangoDB", "workload": "Filtered_Lookup", **stats})

    return results

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main():
    all_results = []
    
    cypher_targets = {
        "CognoDB Cloud": (os.getenv("COGNODB_URI"), os.getenv("COGNODB_USER"), os.getenv("COGNODB_PASSWORD")),
        "Local Neo4j": (os.getenv("NEO4J_URI"), os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD")),
        "Local Memgraph": (os.getenv("MEMGRAPH_URI"), os.getenv("MEMGRAPH_USER") or "", os.getenv("MEMGRAPH_PASSWORD") or "")
    }

    for name, (uri, u, p) in cypher_targets.items():
        print(f"\n--- Running Lookup Benchmarks on {name} ---")
        all_results.extend(run_cypher_lookups(name, uri, u, p))

    print("\n--- Running Lookup Benchmarks on Local FalkorDB ---")
    all_results.extend(run_falkordb_lookups(os.getenv("FALKORDB_HOST", "localhost"), os.getenv("FALKORDB_PORT", 6379)))

    print("\n--- Running Lookup Benchmarks on Local ArangoDB ---")
    all_results.extend(run_arangodb_lookups(os.getenv("ARANGODB_URL", "http://localhost:8529"), os.getenv("ARANGODB_USER", "root"), os.getenv("ARANGODB_PASSWORD", "password123")))

    os.makedirs("results", exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(all_results, f, indent=2)

    print(f"\nLookup benchmark complete! Results written to {RESULTS_PATH}")

if __name__ == "__main__":
    main()