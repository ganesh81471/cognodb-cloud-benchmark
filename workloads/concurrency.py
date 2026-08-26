"""
workloads/concurrency.py

Runs mixed OLTP workloads (80% point-lookup reads / 20% edge-creation writes)
across 1, 10, and 40 concurrent clients using a duration-based measurement window.

All synthetic write relationships are tagged `synthetic: true` and purged in a
single post-sweep cleanup pass to maintain database state integrity.
"""

import os
import sys
import time
import json
import random
import statistics
import threading
from concurrent.futures import ThreadPoolExecutor
import redis
from neo4j import GraphDatabase
from arango import ArangoClient
from dotenv import load_dotenv

load_dotenv()

RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)
CONCURRENCY_FILE = os.path.join(RESULTS_DIR, "concurrency_results.json")

# Benchmark Configuration
CONCURRENCY_LEVELS = [1, 10, 40]
WARMUP_SEC = 3
MEASURED_SEC = 10
READ_RATIO = 0.80  # 80% Read / 20% Write

# Target ID sample spaces matching existing dataset shape
# Use actual dataset IDs: integers matching loaded userId/movieId values
USER_IDS = list(range(1, 600))           # userId range: 1-610 (smaller sample for benchmark)
MOVIE_IDS = list(range(1, 9000))         # movieId range: 1-131262 (smaller sample for benchmark)


def calc_stats(latencies_ms, duration_sec, read_ops, write_ops):
    """Calculates standardized metrics matching project result format."""
    total_ops = read_ops + write_ops
    if not latencies_ms or duration_sec <= 0:
        return {
            "throughput_qps": 0.0,
            "p50_latency_ms": 0.0,
            "p95_latency_ms": 0.0,
            "p99_latency_ms": 0.0,
            "read_ops": 0,
            "write_ops": 0,
            "total_ops": 0
        }
    
    sorted_lat = sorted(latencies_ms)
    n = len(sorted_lat)
    p50 = statistics.median(sorted_lat)
    p95 = sorted_lat[int(0.95 * n)] if n > 0 else 0.0
    p99 = sorted_lat[int(0.99 * n)] if n > 0 else 0.0
    qps = total_ops / duration_sec

    return {
        "throughput_qps": round(qps, 2),
        "p50_latency_ms": round(p50, 2),
        "p95_latency_ms": round(p95, 2),
        "p99_latency_ms": round(p99, 2),
        "read_ops": read_ops,
        "write_ops": write_ops,
        "total_ops": total_ops
    }


# -----------------------------------------------------------------------------
# 1. Cypher Worker Implementation (Neo4j, Memgraph, CognoDB)
# -----------------------------------------------------------------------------
def cypher_worker(uri, auth, stop_event, measure_event, thread_results):
    latencies = []
    r_count = 0
    w_count = 0
    
    try:
        driver = GraphDatabase.driver(uri, auth=auth)
        with driver.session() as session:
            while not stop_event.is_set():
                is_read = random.random() < READ_RATIO
                mid = random.choice(MOVIE_IDS)
                uid = random.choice(USER_IDS)
                rating = round(random.uniform(1.0, 5.0), 1)
                
                t0 = time.perf_counter()
                try:
                    if is_read:
                        res = session.run("MATCH (m:Movie {movieId: $mid}) RETURN m.title", mid=mid)
                        _ = res.single()
                    else:
                        session.run(
                            "MATCH (u:User {userId: $uid}), (m:Movie {movieId: $mid}) "
                            "CREATE (u)-[r:RATED {rating: $rating, synthetic: true}]->(m)",
                            uid=uid, mid=mid, rating=rating
                        )
                    elapsed_ms = (time.perf_counter() - t0) * 1000.0
                    
                    if measure_event.is_set():
                        latencies.append(elapsed_ms)
                        if is_read:
                            r_count += 1
                        else:
                            w_count += 1
                except Exception as e:
                    if measure_event.is_set():
                        print(f"  [Cypher Query Error]: {e}")
                    time.sleep(0.01)
        driver.close()
    except Exception as e:
        print(f"  [Cypher Driver Error]: {e}")

    thread_results.append((latencies, r_count, w_count))


def cleanup_cypher(uri, auth):
    """Purges all synthetic edges created during write phases."""
    try:
        driver = GraphDatabase.driver(uri, auth=auth)
        with driver.session() as session:
            session.run("MATCH ()-[r:RATED {synthetic: true}]->() DELETE r")
        driver.close()
    except Exception as e:
        print(f"  [Cleanup Warning] Cypher synthetic purge failed: {e}")


# -----------------------------------------------------------------------------
# 2. FalkorDB Worker Implementation
# -----------------------------------------------------------------------------
def falkor_worker(host, port, stop_event, measure_event, thread_results):
    latencies = []
    r_count = 0
    w_count = 0

    try:
        r = redis.Redis(host=host, port=port, decode_responses=True)
        while not stop_event.is_set():
            is_read = random.random() < READ_RATIO
            mid = random.choice(MOVIE_IDS)
            uid = random.choice(USER_IDS)
            rating = round(random.uniform(1.0, 5.0), 1)

            t0 = time.perf_counter()
            try:
                if is_read:
                    q = f"MATCH (m:Movie {{movieId: {mid}}}) RETURN m.title"
                    r.execute_command("GRAPH.QUERY", "movielens", q)
                else:
                    q = f"MATCH (u:User {{userId: {uid}}}), (m:Movie {{movieId: {mid}}}) CREATE (u)-[r:RATED {{rating: {rating}, synthetic: true}}]->(m)"
                    r.execute_command("GRAPH.QUERY", "movielens", q)
                elapsed_ms = (time.perf_counter() - t0) * 1000.0

                if measure_event.is_set():
                    latencies.append(elapsed_ms)
                    if is_read:
                        r_count += 1
                    else:
                        w_count += 1
            except Exception as e:
                if measure_event.is_set():
                    print(f"  [FalkorDB Query Error]: {e}")
                time.sleep(0.01)
        r.close()
    except Exception as e:
        print(f"  [FalkorDB Client Error]: {e}")

    thread_results.append((latencies, r_count, w_count))


def cleanup_falkor(host, port):
    try:
        r = redis.Redis(host=host, port=port, decode_responses=True)
        r.execute_command("GRAPH.QUERY", "movielens", "MATCH ()-[r:RATED {synthetic: true}]->() DELETE r")
        r.close()
    except Exception as e:
        print(f"  [Cleanup Warning] FalkorDB synthetic purge failed: {e}")


# -----------------------------------------------------------------------------
# 3. ArangoDB Worker Implementation
# -----------------------------------------------------------------------------
def arango_worker(url, user, password, stop_event, measure_event, thread_results):
    latencies = []
    r_count = 0
    w_count = 0

    try:
        client = ArangoClient(hosts=url)
        db = client.db("movielens_db", username=user or None, password=password or None)
        
        # Auto-detect edge collection name ('ratings' vs 'rated')
        edge_coll = "ratings" if db.has_collection("ratings") else "rated"

        read_aql = "FOR m IN Movie FILTER m.movieId == @mid RETURN {title: m.title}"
        write_aql = f"""
    FOR u IN User FILTER u.userId == @uid
      FOR m IN Movie FILTER m.movieId == @mid
        INSERT {{ _from: u._id, _to: m._id, rating: @rating, synthetic: true }} INTO {edge_coll}
    """

        while not stop_event.is_set():
            is_read = random.random() < READ_RATIO
            mid = random.choice(MOVIE_IDS)
            uid = random.choice(USER_IDS)
            rating = round(random.uniform(1.0, 5.0), 1)

            t0 = time.perf_counter()
            try:
                if is_read:
                    cursor = db.aql.execute(read_aql, bind_vars={"mid": mid})
                    _ = list(cursor)
                else:
                    db.aql.execute(write_aql, bind_vars={"uid": uid, "mid": mid, "rating": rating})
                elapsed_ms = (time.perf_counter() - t0) * 1000.0

                if measure_event.is_set():
                    latencies.append(elapsed_ms)
                    if is_read:
                        r_count += 1
                    else:
                        w_count += 1
            except Exception as e:
                if measure_event.is_set():
                    print(f"  [ArangoDB Query Error]: {e}")
                time.sleep(0.01)
    except Exception as e:
        print(f"  [ArangoDB Client Error]: {e}")

    thread_results.append((latencies, r_count, w_count))


def cleanup_arango(url, user, password):
    try:
        client = ArangoClient(hosts=url)
        db = client.db("movielens_db", username=user or None, password=password or None)
        edge_coll = "RATED" if db.has_collection("RATED") else ("ratings" if db.has_collection("ratings") else "rated")
        if db.has_collection(edge_coll):
            db.aql.execute(f"FOR e IN {edge_coll} FILTER e.synthetic == true REMOVE e IN {edge_coll}")
            print(f"  [ArangoDB Cleanup] Purged synthetic edges from '{edge_coll}'.")
    except Exception as e:
        print(f"  [Cleanup Warning] ArangoDB synthetic purge failed: {e}")


# -----------------------------------------------------------------------------
# Workload Runner Orchestrator
# -----------------------------------------------------------------------------
def run_concurrency_sweep(platform_name, worker_func, cleanup_func, worker_args):
    print(f"\n[{platform_name}] Starting Concurrency Sweep (80% Read / 20% Write)...")
    sweep_results = []

    for clients in CONCURRENCY_LEVELS:
        stop_event = threading.Event()
        measure_event = threading.Event()
        thread_results = []

        executor = ThreadPoolExecutor(max_workers=clients)
        futures = [
            executor.submit(worker_func, *worker_args, stop_event, measure_event, thread_results)
            for _ in range(clients)
        ]

        # 1. Warmup Phase
        time.sleep(WARMUP_SEC)

        # 2. Measured Window Phase
        measure_event.set()
        t_start = time.perf_counter()
        time.sleep(MEASURED_SEC)
        t_measured_duration = time.perf_counter() - t_start

        # 3. Teardown
        stop_event.set()
        executor.shutdown(wait=True)

        # 4. Aggregate metrics across threads
        all_latencies = []
        total_read_ops = 0
        total_write_ops = 0
        for lats, r_ops, w_ops in thread_results:
            all_latencies.extend(lats)
            total_read_ops += r_ops
            total_write_ops += w_ops

        stats = calc_stats(all_latencies, t_measured_duration, total_read_ops, total_write_ops)
        stats["concurrency"] = clients
        sweep_results.append(stats)

        print(
            f"  Concurrency {clients:2d} clients -> "
            f"{stats['throughput_qps']:8.2f} QPS | "
            f"p50: {stats['p50_latency_ms']:6.2f} ms | "
            f"p95: {stats['p95_latency_ms']:6.2f} ms"
        )

    # Clean up synthetic write records
    print(f"[{platform_name}] Cleaning up synthetic edges...")
    cleanup_func()

    return {
        "platform": platform_name,
        "workload": "Concurrent Mixed (80% Read / 20% Write)",
        "warmup_sec": WARMUP_SEC,
        "measured_sec": MEASURED_SEC,
        "concurrency_sweep": sweep_results
    }


def main():
    print("=================================================================")
    print("        RUNNING CONCURRENCY BENCHMARK SUITE (80/20 MIX)          ")
    print("=================================================================")
    all_results = []

    # 1. Local Neo4j
    neo4j_auth = (os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password123"))
    all_results.append(run_concurrency_sweep(
        "Local Neo4j", cypher_worker,
        lambda: cleanup_cypher(os.getenv("NEO4J_URI", "bolt://localhost:7687"), neo4j_auth),
        (os.getenv("NEO4J_URI", "bolt://localhost:7687"), neo4j_auth)
    ))

    # 2. Local Memgraph
    memgraph_auth = (os.getenv("MEMGRAPH_USER", ""), os.getenv("MEMGRAPH_PASSWORD", ""))
    all_results.append(run_concurrency_sweep(
        "Local Memgraph", cypher_worker,
        lambda: cleanup_cypher(os.getenv("MEMGRAPH_URI", "bolt://localhost:7688"), memgraph_auth),
        (os.getenv("MEMGRAPH_URI", "bolt://localhost:7688"), memgraph_auth)
    ))

    # 3. Local FalkorDB
    falkor_host = os.getenv("FALKORDB_HOST", "localhost")
    falkor_port = int(os.getenv("FALKORDB_PORT", 6379))
    all_results.append(run_concurrency_sweep(
        "Local FalkorDB", falkor_worker,
        lambda: cleanup_falkor(falkor_host, falkor_port),
        (falkor_host, falkor_port)
    ))

    # 4. Local ArangoDB
    arango_url = os.getenv("ARANGODB_URL", "http://localhost:8529")
    arango_user = os.getenv("ARANGODB_USER", "root")
    arango_pass = os.getenv("ARANGODB_PASSWORD", "password123")
    all_results.append(run_concurrency_sweep(
        "Local ArangoDB", arango_worker,
        lambda: cleanup_arango(arango_url, arango_user, arango_pass),
        (arango_url, arango_user, arango_pass)
    ))

    # 5. CognoDB Cloud
    cogno_uri = os.getenv("COGNODB_URI")
    if cogno_uri:
        cogno_auth = (os.getenv("COGNODB_USER"), os.getenv("COGNODB_PASSWORD"))
        all_results.append(run_concurrency_sweep(
            "CognoDB Cloud", cypher_worker,
            lambda: cleanup_cypher(cogno_uri, cogno_auth),
            (cogno_uri, cogno_auth)
        ))
    else:
        print("\n[CognoDB Cloud] Skipped: COGNODB_URI not set.")

    # Save outputs
    with open(CONCURRENCY_FILE, "w") as f:
        json.dump(all_results, f, indent=4)

    print(f"\nSuccessfully written concurrency results to {CONCURRENCY_FILE}")


if __name__ == "__main__":
    main()