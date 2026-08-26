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
RESULTS_PATH = os.path.join("results", "aggregation_benchmark_results.json")

WARMUP_RUNS = 10
MEASURED_RUNS = 100
QUERY_TIMEOUT_SEC = 15
RANDOM_SEED = 20260825

ratings_df = pd.read_csv(RATINGS_PATH)
USER_IDS = ratings_df['userId'].unique().tolist()


def workload_user_ids(workload, count):
    """Return the same deterministic user sequence on every platform."""
    rng = random.Random(f"{RANDOM_SEED}:{workload}")
    return [rng.choice(USER_IDS) for _ in range(count)]

def calc_stats(durations):
    return {
        "mean_ms": round(float(np.mean(durations)), 2),
        "p50_ms": round(float(np.percentile(durations, 50)), 2),
        "p95_ms": round(float(np.percentile(durations, 95)), 2),
        "min_ms": round(float(np.min(durations)), 2),
        "max_ms": round(float(np.max(durations)), 2)
    }

def skipped_result(platform, q_name, warmup_failures):
    print(f"[{platform}] {q_name} -> SKIPPED ({warmup_failures} warm-up failures)")
    return {
        "platform": platform,
        "workload": q_name,
        "all_runs_failed": True,
        "skipped_after_warmup": True,
        "warmup_failures": warmup_failures,
        "success_rate": 0.0,
    }

def measured_result(platform, q_name, durations, failures, total_attempts):
    if durations:
        stats = calc_stats(durations)
        stats["failed_runs"] = failures
        stats["success_rate"] = round(len(durations) / total_attempts, 3)
        print(f"[{platform}] {q_name} -> p50: {stats['p50_ms']}ms | p95: {stats['p95_ms']}ms | failures: {failures}")
        return {"platform": platform, "workload": q_name, **stats}
    else:
        print(f"[{platform}] {q_name} -> ALL {total_attempts} RUNS FAILED")
        return {
            "platform": platform, "workload": q_name,
            "all_runs_failed": True, "failed_runs": failures, "success_rate": 0.0
        }

# -----------------------------------------------------------------------------
# 1. CYPHER AGGREGATIONS (CognoDB, Neo4j, Memgraph)
# -----------------------------------------------------------------------------
def run_cypher_aggregations(platform, uri, user, password):
    if not uri:
        return []

    auth = (user, password) if user or password else None
    driver = GraphDatabase.driver(uri, auth=auth)
    results = []

    queries = {
        "Global_Avg_Rating": "MATCH (:User)-[r:RATED]->(:Movie) RETURN AVG(r.rating) AS avg_rating",
        "Rating_Distribution": """
            MATCH (:User)-[r:RATED]->(:Movie)
            RETURN r.rating AS rating, COUNT(*) AS rating_count
            ORDER BY rating ASC
        """,
        "User_Genre_Agg": """
            MATCH (u:User {userId: $id})-[r:RATED]->(m:Movie)-[:HAS_GENRE]->(g:Genre)
            RETURN g.name, AVG(r.rating) AS avg_rating, COUNT(m) AS count
            ORDER BY avg_rating DESC
        """
    }

    def fresh_session():
        nonlocal driver
        try:
            driver.close()
        except Exception:
            pass
        driver = GraphDatabase.driver(uri, auth=auth)
        return driver.session()

    session = driver.session()

    for q_name, cypher_stmt in queries.items():
        user_ids = workload_user_ids(q_name, WARMUP_RUNS + MEASURED_RUNS)

        def run_once(active_session, statement, u_id):
            res = active_session.run(statement, id=u_id, timeout=QUERY_TIMEOUT_SEC)
            return list(res)

        warmup_failures = 0
        for u_id in user_ids[:WARMUP_RUNS]:
            try:
                run_once(session, cypher_stmt, u_id)
            except Exception as e:
                warmup_failures += 1
                print(f"[{platform}] {q_name} warm-up failed: {e}")

        if warmup_failures == WARMUP_RUNS:
            results.append(skipped_result(platform, q_name, warmup_failures))
            session = fresh_session()
            continue

        durations = []
        failures = 0
        for run_number, u_id in enumerate(user_ids[WARMUP_RUNS:], start=1):
            try:
                start = time.perf_counter()
                run_once(session, cypher_stmt, u_id)
                durations.append((time.perf_counter() - start) * 1000)
            except Exception as e:
                failures += 1
                print(f"[{platform}] {q_name} run {run_number} failed: {e}")
                session = fresh_session()

        results.append(measured_result(platform, q_name, durations, failures, MEASURED_RUNS))

    try:
        session.close()
    except Exception:
        pass
    driver.close()
    return results

# -----------------------------------------------------------------------------
# 2. FALKORDB AGGREGATIONS
# -----------------------------------------------------------------------------
def run_falkordb_aggregations(host, port):
    def fresh_client():
        return redis.Redis(host=host, port=int(port), socket_timeout=QUERY_TIMEOUT_SEC)

    r = fresh_client()
    graph_name = "movielens"
    results = []

    queries = {
        "Global_Avg_Rating": "MATCH (:User)-[r:RATED]->(:Movie) RETURN AVG(r.rating) AS avg_rating",
        "Rating_Distribution": """
            MATCH (:User)-[r:RATED]->(:Movie)
            RETURN r.rating AS rating, COUNT(*) AS rating_count
            ORDER BY rating ASC
        """,
        "User_Genre_Agg": """
            MATCH (u:User {{userId: {id}}})-[r:RATED]->(m:Movie)-[:HAS_GENRE]->(g:Genre)
            RETURN g.name, AVG(r.rating) AS avg_rating, COUNT(m) AS count
            ORDER BY avg_rating DESC
        """
    }

    for q_name, cypher_stmt in queries.items():
        user_ids = workload_user_ids(q_name, WARMUP_RUNS + MEASURED_RUNS)

        def run_once(client, statement, u_id):
            query = statement.format(id=u_id) if "{id}" in statement else statement
            return client.execute_command("GRAPH.QUERY", graph_name, query)

        warmup_failures = 0
        for u_id in user_ids[:WARMUP_RUNS]:
            try:
                run_once(r, cypher_stmt, u_id)
            except Exception as e:
                warmup_failures += 1
                print(f"[Local FalkorDB] {q_name} warm-up failed: {e}")

        if warmup_failures == WARMUP_RUNS:
            results.append(skipped_result("Local FalkorDB", q_name, warmup_failures))
            r = fresh_client()
            continue

        durations = []
        failures = 0
        for run_number, u_id in enumerate(user_ids[WARMUP_RUNS:], start=1):
            try:
                start = time.perf_counter()
                run_once(r, cypher_stmt, u_id)
                durations.append((time.perf_counter() - start) * 1000)
            except Exception as e:
                failures += 1
                print(f"[Local FalkorDB] {q_name} run {run_number} failed: {e}")
                r = fresh_client()

        results.append(measured_result("Local FalkorDB", q_name, durations, failures, MEASURED_RUNS))

    return results

# -----------------------------------------------------------------------------
# 3. ARANGODB AGGREGATIONS (AQL)
# -----------------------------------------------------------------------------
def run_arangodb_aggregations(url, user, password):
    def fresh_db():
        client = ArangoClient(hosts=url)
        return client.db("movielens_db", username=user, password=password)

    db = fresh_db()
    results = []

    queries = {
        "Global_Avg_Rating": "FOR r IN RATED COLLECT AGGREGATE avg_rating = AVG(r.rating) RETURN avg_rating",
        "Rating_Distribution": """
            FOR r IN RATED
            COLLECT rating = r.rating AGGREGATE rating_count = COUNT(r)
            SORT rating ASC
            RETURN { rating: rating, rating_count: rating_count }
        """,
        "User_Genre_Agg": """
            FOR r IN RATED
            FILTER r._from == CONCAT('User/', @id)
            FOR g IN 1..1 OUTBOUND r._to HAS_GENRE
            COLLECT genre = g.name AGGREGATE count = COUNT(r), avg_rating = AVG(r.rating)
            SORT avg_rating DESC
            RETURN { genre: genre, avg_rating: avg_rating, count: count }
        """
    }

    for q_name, aql_stmt in queries.items():
        user_ids = workload_user_ids(q_name, WARMUP_RUNS + MEASURED_RUNS)

        def run_once(active_db, statement, u_id):
            bind_vars = {'id': u_id} if '@id' in statement else None
            cursor = active_db.aql.execute(statement, bind_vars=bind_vars, max_runtime=QUERY_TIMEOUT_SEC)
            return list(cursor)

        warmup_failures = 0
        for u_id in user_ids[:WARMUP_RUNS]:
            try:
                run_once(db, aql_stmt, u_id)
            except Exception as e:
                warmup_failures += 1
                print(f"[Local ArangoDB] {q_name} warm-up failed: {e}")

        if warmup_failures == WARMUP_RUNS:
            results.append(skipped_result("Local ArangoDB", q_name, warmup_failures))
            db = fresh_db()
            continue

        durations = []
        failures = 0
        for run_number, u_id in enumerate(user_ids[WARMUP_RUNS:], start=1):
            try:
                start = time.perf_counter()
                run_once(db, aql_stmt, u_id)
                durations.append((time.perf_counter() - start) * 1000)
            except Exception as e:
                failures += 1
                print(f"[Local ArangoDB] {q_name} run {run_number} failed: {e}")
                db = fresh_db()

        results.append(measured_result("Local ArangoDB", q_name, durations, failures, MEASURED_RUNS))

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
        print(f"\n--- Running Aggregations on {name} ---")
        try:
            all_results.extend(run_cypher_aggregations(name, uri, u, p))
        except Exception as e:
            print(f"Fatal error on {name}: {e}")

    print("\n--- Running Aggregations on Local FalkorDB ---")
    try:
        all_results.extend(run_falkordb_aggregations(os.getenv("FALKORDB_HOST", "localhost"), os.getenv("FALKORDB_PORT", 6379)))
    except Exception as e:
        print(f"Fatal error on FalkorDB: {e}")

    print("\n--- Running Aggregations on Local ArangoDB ---")
    try:
        all_results.extend(run_arangodb_aggregations(os.getenv("ARANGODB_URL", "http://localhost:8529"), os.getenv("ARANGODB_USER", "root"), os.getenv("ARANGODB_PASSWORD", "password123")))
    except Exception as e:
        print(f"Fatal error on ArangoDB: {e}")

    os.makedirs("results", exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(all_results, f, indent=2)

    print(f"\nAggregation benchmark complete! Results written to {RESULTS_PATH}")

if __name__ == "__main__":
    main()
