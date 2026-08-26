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
RESULTS_PATH = os.path.join("results", "traversal_benchmark_results.json")

WARMUP_RUNS = 10
MEASURED_RUNS = 100
QUERY_TIMEOUT_SEC = 15  # Server-side timeout, where the platform honors it.
BENCHMARK_SEED = 42

# Applied identically to every platform's 3-hop query. This keeps the workload
# tractable on the smallest free-tier resource envelope; it is a deliberate
# workload definition, not a per-platform optimization.
THREE_HOP_CO_RATER_LIMIT = 50

# Load User IDs to randomize traversal start points
ratings_df = pd.read_csv(RATINGS_PATH)
USER_IDS = ratings_df['userId'].unique().tolist()
randomizer = random.Random(BENCHMARK_SEED)
WARMUP_USER_IDS = [randomizer.choice(USER_IDS) for _ in range(WARMUP_RUNS)]
MEASURED_USER_IDS = [randomizer.choice(USER_IDS) for _ in range(MEASURED_RUNS)]


def calc_stats(durations):
    return {
        "mean_ms": round(float(np.mean(durations)), 2),
        "p50_ms": round(float(np.percentile(durations, 50)), 2),
        "p95_ms": round(float(np.percentile(durations, 95)), 2),
        "min_ms": round(float(np.min(durations)), 2),
        "max_ms": round(float(np.max(durations)), 2)
    }


def skipped_result(platform, q_name, warmup_failures):
    print(f"[{platform}] {q_name} -> SKIPPED, all {warmup_failures} warm-up attempts failed/timed out")
    return {
        "platform": platform,
        "workload": q_name,
        "all_runs_failed": True,
        "skipped_after_warmup": True,
        "warmup_failures": warmup_failures,
        "success_rate": 0.0,
    }


def unavailable_results(platform, reason):
    return [
        {
            "platform": platform,
            "workload": q_name,
            "skipped": True,
            "skip_reason": reason,
            "success_rate": 0.0,
        }
        for q_name in ("1_Hop", "2_Hop", "3_Hop")
    ]


def measured_result(platform, q_name, durations, failures, total_attempts):
    if durations:
        stats = calc_stats(durations)
        stats["failed_runs"] = failures
        stats["success_rate"] = round(len(durations) / total_attempts, 3)
        print(f"[{platform}] {q_name} -> p50: {stats['p50_ms']}ms | p95: {stats['p95_ms']}ms | failures: {failures}")
        return {"platform": platform, "workload": q_name, **stats}
    else:
        print(f"[{platform}] {q_name} -> ALL {total_attempts} RUNS FAILED (likely timeout)")
        return {
            "platform": platform, "workload": q_name,
            "all_runs_failed": True, "failed_runs": failures, "success_rate": 0.0
        }


# -----------------------------------------------------------------------------
# 1. CYPHER TRAVERSALS (CognoDB, Neo4j, Memgraph)
# -----------------------------------------------------------------------------
def run_cypher_traversals(platform, uri, user, password):
    if not uri:
        print(f"Skipping {platform}: URI missing.")
        return []

    auth = (user, password) if user or password else None
    driver = GraphDatabase.driver(uri, auth=auth)
    results = []

    queries = {
        "1_Hop": "MATCH (u:User {userId: $id})-[:RATED]->(m:Movie) RETURN m.movieId",
        "2_Hop": "MATCH (u:User {userId: $id})-[:RATED]->(:Movie)<-[:RATED]-(other:User) WHERE other.userId <> $id RETURN DISTINCT other.userId LIMIT 100",
        "3_Hop": """
            MATCH (u:User {userId: $id})-[:RATED]->(:Movie)<-[:RATED]-(other:User)
            WHERE other.userId <> $id
            WITH DISTINCT u, other
            ORDER BY other.userId
            LIMIT $co_rater_limit
            MATCH (other)-[:RATED]->(rec:Movie)
            WHERE NOT (u)-[:RATED]->(rec)
            RETURN rec.title, COUNT(*) AS score
            ORDER BY score DESC LIMIT 10
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

        def run_once(active_session, statement, u_id):
            res = active_session.run(
                statement,
                id=u_id,
                co_rater_limit=THREE_HOP_CO_RATER_LIMIT,
                timeout=QUERY_TIMEOUT_SEC,
            )
            return list(res)

        warmup_failures = 0
        for _ in range(WARMUP_RUNS):
            try:
                run_once(session, cypher_stmt, random.choice(USER_IDS))
            except Exception as e:
                warmup_failures += 1
                print(f"[{platform}] {q_name} warm-up failed: {e}")

        # Fail-fast: if every warm-up attempt failed (server timeout or crash),
        # this query is structurally too
        # expensive for this platform under current resource limits. Skip
        # measured runs and get a fresh connection before the next query,
        # since a server-aborted or hung Bolt session can be left unusable.
        if warmup_failures == WARMUP_RUNS:
            results.append(skipped_result(platform, q_name, warmup_failures))
            try:
                session.close()
            except Exception:
                pass
            session = fresh_session()
            continue

        durations = []
        failures = 0
        for _ in range(MEASURED_RUNS):
            u_id = random.choice(USER_IDS)
            try:
                start = time.perf_counter()
                run_once(session, cypher_stmt, u_id)
                durations.append((time.perf_counter() - start) * 1000)
            except Exception:
                failures += 1
                # If the connection died mid-run, recover so remaining
                # iterations (and the next query) aren't lost too.
                try:
                    session.close()
                except Exception:
                    pass
                session = fresh_session()

        results.append(measured_result(platform, q_name, durations, failures, MEASURED_RUNS))

    try:
        session.close()
    except Exception:
        pass
    driver.close()
    return results


# -----------------------------------------------------------------------------
# 2. FALKORDB TRAVERSALS
# -----------------------------------------------------------------------------
def run_falkordb_traversals(host, port):
    def fresh_client():
        return redis.Redis(host=host, port=int(port), socket_timeout=QUERY_TIMEOUT_SEC)

    r = fresh_client()
    graph_name = "movielens"
    results = []

    queries = {
        "1_Hop": "MATCH (u:User {{userId: {id}}})-[:RATED]->(m:Movie) RETURN m.movieId",
        "2_Hop": "MATCH (u:User {{userId: {id}}})-[:RATED]->(:Movie)<-[:RATED]-(other:User) WHERE other.userId <> {id} RETURN DISTINCT other.userId LIMIT 100",
        "3_Hop": """
            MATCH (u:User {{userId: {id}}})-[:RATED]->(:Movie)<-[:RATED]-(other:User)
            WHERE other.userId <> {id}
            WITH DISTINCT u, other
            ORDER BY other.userId
            LIMIT {co_rater_limit}
            MATCH (other)-[:RATED]->(rec:Movie)
            WHERE NOT (u)-[:RATED]->(rec)
            RETURN rec.title, COUNT(*) AS score
            ORDER BY score DESC LIMIT 10
        """
    }

    for q_name, cypher_stmt in queries.items():

        def run_once(client, statement, u_id):
            query = statement.format(id=u_id, co_rater_limit=THREE_HOP_CO_RATER_LIMIT)
            return client.execute_command("GRAPH.QUERY", graph_name, query)

        warmup_failures = 0
        for _ in range(WARMUP_RUNS):
            try:
                run_once(r, cypher_stmt, random.choice(USER_IDS))
            except Exception as e:
                warmup_failures += 1
                print(f"[Local FalkorDB] {q_name} warm-up failed: {e}")

        if warmup_failures == WARMUP_RUNS:
            results.append(skipped_result("Local FalkorDB", q_name, warmup_failures))
            r = fresh_client()  # discard a possibly-wedged connection
            continue

        durations = []
        failures = 0
        for _ in range(MEASURED_RUNS):
            u_id = random.choice(USER_IDS)
            try:
                start = time.perf_counter()
                run_once(r, cypher_stmt, u_id)
                durations.append((time.perf_counter() - start) * 1000)
            except Exception:
                failures += 1
                r = fresh_client()

        results.append(measured_result("Local FalkorDB", q_name, durations, failures, MEASURED_RUNS))

    return results


# -----------------------------------------------------------------------------
# 3. ARANGODB TRAVERSALS (AQL)
# -----------------------------------------------------------------------------
def run_arangodb_traversals(url, user, password):
    def fresh_db():
        client = ArangoClient(hosts=url, request_timeout=5)
        return client.db("movielens_db", username=user, password=password)

    db = fresh_db()
    results = []

    queries = {
        "1_Hop": "FOR m IN 1..1 OUTBOUND CONCAT('User/', @id) RATED RETURN m.movieId",
        "2_Hop": """
            FOR m IN 1..1 OUTBOUND CONCAT('User/', @id) RATED
            FOR u IN 1..1 INBOUND m._id RATED
            FILTER u._id != CONCAT('User/', @id)
            COLLECT userId = u.userId
            LIMIT 100
            RETURN userId
        """,
        "3_Hop": """
            LET already_rated = (
                FOR r IN RATED FILTER r._from == CONCAT('User/', @id) RETURN r._to
            )
            FOR m IN 1..1 OUTBOUND CONCAT('User/', @id) RATED
            FOR other IN 1..1 INBOUND m._id RATED
            FILTER other._id != CONCAT('User/', @id)
            COLLECT other_id = other._id, other_user_id = other.userId
            SORT other_user_id ASC
            LIMIT @co_rater_limit
            FOR rec IN 1..1 OUTBOUND other_id RATED
            FILTER rec._id NOT IN already_rated
            COLLECT movie = rec.title WITH COUNT INTO score
            SORT score DESC LIMIT 10
            RETURN { title: movie, score: score }
        """
    }

    for q_name, aql_stmt in queries.items():

        def run_once(active_db, statement, u_id):
            bind_vars = {'id': u_id}
            if '@co_rater_limit' in statement:
                bind_vars['co_rater_limit'] = THREE_HOP_CO_RATER_LIMIT
            cursor = active_db.aql.execute(
                statement,
                bind_vars=bind_vars,
                max_runtime=QUERY_TIMEOUT_SEC,
            )
            return list(cursor)

        warmup_failures = 0
        for _ in range(WARMUP_RUNS):
            try:
                run_once(db, aql_stmt, random.choice(USER_IDS))
            except Exception as e:
                warmup_failures += 1
                print(f"[Local ArangoDB] {q_name} warm-up failed: {e}")

        if warmup_failures == WARMUP_RUNS:
            results.append(skipped_result("Local ArangoDB", q_name, warmup_failures))
            db = fresh_db()
            continue

        durations = []
        failures = 0
        for _ in range(MEASURED_RUNS):
            u_id = random.choice(USER_IDS)
            try:
                start = time.perf_counter()
                run_once(db, aql_stmt, u_id)
                durations.append((time.perf_counter() - start) * 1000)
            except Exception:
                failures += 1
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
        print(f"\n--- Running Traversals on {name} ---")
        try:
            all_results.extend(run_cypher_traversals(name, uri, u, p))
        except Exception as e:
            print(f"Fatal error on {name}, moving to next platform: {e}")

    print("\n--- Running Traversals on Local FalkorDB ---")
    try:
        all_results.extend(run_falkordb_traversals(os.getenv("FALKORDB_HOST", "localhost"), os.getenv("FALKORDB_PORT", 6379)))
    except Exception as e:
        print(f"Fatal error on FalkorDB, moving to next platform: {e}")

    print("\n--- Running Traversals on Local ArangoDB ---")
    try:
        all_results.extend(run_arangodb_traversals(os.getenv("ARANGODB_URL", "http://localhost:8529"), os.getenv("ARANGODB_USER", "root"), os.getenv("ARANGODB_PASSWORD", "password123")))
    except Exception as e:
        print(f"Fatal error on ArangoDB: {e}")

    os.makedirs("results", exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(all_results, f, indent=2)

    print(f"\nTraversal benchmark complete! Results written to {RESULTS_PATH}")


if __name__ == "__main__":
    main()
