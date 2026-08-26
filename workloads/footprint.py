import os
import json
import redis
from neo4j import GraphDatabase
from arango import ArangoClient
from dotenv import load_dotenv

load_dotenv()

RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)
FOOTPRINT_FILE = os.path.join(RESULTS_DIR, "footprint_results.json")


# -----------------------------------------------------------------------------
# 1. NEO4J
# -----------------------------------------------------------------------------
def get_neo4j_footprint(uri, user, password, name):
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password) if user else None)
        with driver.session() as session:
            store_size = None
            try:
                result = session.run(
                    "SHOW DATABASES YIELD name, currentStatus, storeSize "
                    "WHERE name = 'neo4j' RETURN storeSize"
                )
                record = result.single()
                if record and "storeSize" in record.keys():
                    store_size = record["storeSize"]
            except Exception:
                # storeSize is not exposed by this Neo4j edition/version via
                # SHOW DATABASES — a genuine platform limitation, not a bug.
                # Confirmed via: Neo.ClientError.Statement.SyntaxError
                # "Trying to YIELD non-existing column: storeSize"
                store_size = None

            if isinstance(store_size, (int, float)):
                value = f"{store_size / (1024 * 1024):.2f} MB"
            else:
                value = "Not observable via Cypher (storeSize not supported on this Neo4j edition/version)"

            driver.close()
            return {"platform": name, "metric": "Store Size", "value": value}
    except Exception as e:
        return {"platform": name, "metric": "Store Size", "value": f"Not observable: connection error ({e})"}


# -----------------------------------------------------------------------------
# 2. MEMGRAPH
# -----------------------------------------------------------------------------
def get_memgraph_footprint():
    try:
        user = os.getenv("MEMGRAPH_USER") or ""
        password = os.getenv("MEMGRAPH_PASSWORD") or ""
        driver = GraphDatabase.driver(os.getenv("MEMGRAPH_URI"), auth=(user, password))
        with driver.session() as session:
            result = session.run("SHOW STORAGE INFO;")
            records = list(result)

            memory_usage = "Not observable"
            for record in records:
                # Memgraph's SHOW STORAGE INFO column headers vary across
                # versions — read positionally rather than trusting an
                # exact label, and match loosely on "memory" in the label.
                keys = list(record.keys())
                if len(keys) < 2:
                    continue
                label = str(record[keys[0]])
                value = record[keys[1]]

                if "memory" in label.lower():
                    value_str = str(value)
                    try:
                        mem_bytes = float(value)
                        memory_usage = f"{mem_bytes / (1024 * 1024):.2f} MB (metric: {label})"
                    except (TypeError, ValueError):
                        # Memgraph often pre-formats this value as a string
                        # with its own unit (e.g. "116.72MiB") rather than
                        # a raw byte count — use it as-is instead of
                        # mislabeling it "unit unclear".
                        memory_usage = f"{value_str} (metric: {label}, platform-formatted)"
                    break

        driver.close()
        return {"platform": "Local Memgraph", "metric": "Allocated RAM", "value": memory_usage}
    except Exception as e:
        return {"platform": "Local Memgraph", "metric": "Allocated RAM", "value": f"Error: {e}"}


# -----------------------------------------------------------------------------
# 3. FALKORDB
# -----------------------------------------------------------------------------
def get_falkordb_footprint():
    graph_name = "movielens"
    try:
        r = redis.Redis(
            host=os.getenv("FALKORDB_HOST", "localhost"),
            port=int(os.getenv("FALKORDB_PORT", 6379)),
            decode_responses=True,
        )

        # GRAPH.MEMORY USAGE <graph> returns a flat RESP array of
        # alternating field names and values, e.g.:
        #   ["total_graph_sz_mb", 1086, "label_matrices_sz_mb", 96, ...]
        info = r.execute_command("GRAPH.MEMORY", "USAGE", graph_name)

        memory_usage = "Not observable"
        try:
            if isinstance(info, (list, tuple)) and len(info) >= 2:
                for i in range(0, len(info) - 1, 2):
                    field_name = info[i]
                    field_value = info[i + 1]
                    if field_name == "total_graph_sz_mb":
                        try:
                            val = float(field_value)
                            memory_usage = f"{val:.2f} MB"
                        except (TypeError, ValueError):
                            memory_usage = f"{field_value} MB"
                        break
            elif isinstance(info, dict):
                if "total_graph_sz_mb" in info:
                    try:
                        val = float(info["total_graph_sz_mb"])
                        memory_usage = f"{val:.2f} MB"
                    except (TypeError, ValueError):
                        memory_usage = f"{info['total_graph_sz_mb']} MB"
        except Exception:
            memory_usage = "Not observable"

        return {"platform": "Local FalkorDB", "metric": "Graph Memory Usage (total_graph_sz_mb)", "value": memory_usage}
    except Exception as e:
        return {"platform": "Local FalkorDB", "metric": "Graph Memory Usage", "value": f"Error: {e}"}


# -----------------------------------------------------------------------------
# 4. ARANGODB
# -----------------------------------------------------------------------------
def extract_arangodb_size(stats):
    """
    ArangoDB/python-arango's Collection.statistics() return shape varies
    across versions/engines. Try known candidate keys in priority order;
    returns (size_in_bytes, key_used) or (0, None) if nothing matched.
    """
def extract_arangodb_size(stats):
    candidate_keys = ["documents_size", "documentsSize", "memory", "diskSize", "datafileSize"]
    for key in candidate_keys:
        if key in stats and isinstance(stats[key], (int, float)) and stats[key] > 0:
            return stats[key], key
    return 0, None


def get_arangodb_footprint():
    try:
        client = ArangoClient(hosts=os.getenv("ARANGODB_URL", "http://localhost:8529"))
        arango_user = os.getenv("ARANGODB_USER")
        arango_pass = os.getenv("ARANGODB_PASSWORD")
        db = client.db("movielens_db", username=arango_user or None, password=arango_pass or None)

        total_bytes = 0
        matched_key = None
        debug_printed = False

        for col in db.collections():
            if not col['name'].startswith('_'):
                stats = db.collection(col['name']).statistics()

                if not debug_printed:
                    # One-time diagnostic: prints the real key names this
                    # ArangoDB version actually returns, so if none of the
                    # candidate_keys above match, we know exactly what to
                    # add instead of guessing further.
                    print(f"[DEBUG] ArangoDB statistics() keys for '{col['name']}': {list(stats.keys())}")
                    debug_printed = True

                size, key_used = extract_arangodb_size(stats)
                total_bytes += size
                matched_key = matched_key or key_used

        if total_bytes > 0:
            total_mb = f"{total_bytes / (1024 * 1024):.2f} MB (metric: {matched_key})"
        else:
            total_mb = "Not observable (no matching size field found in collection statistics — see [DEBUG] output for actual keys)"

        return {"platform": "Local ArangoDB", "metric": "Collection Memory", "value": total_mb}
    except Exception as e:
        return {"platform": "Local ArangoDB", "metric": "Collection Memory", "value": f"Error: {e}"}


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main():
    print("Gathering footprint metrics...")
    results = []

    results.append(get_neo4j_footprint(os.getenv("NEO4J_URI"), os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"), "Local Neo4j"))
    results.append(get_memgraph_footprint())
    results.append(get_falkordb_footprint())
    results.append(get_arangodb_footprint())

    # CognoDB is a cloud service without a standard admin footprint endpoint
    # exposed to this driver — explicitly disclosed per assignment guidance
    # ("say 'not observable' where it is").
    results.append({"platform": "CognoDB Cloud", "metric": "Storage Size", "value": "Not observable via API (Cloud Hosted)"})

    with open(FOOTPRINT_FILE, "w") as f:
        json.dump(results, f, indent=4)

    print("\n--- Footprint Results ---")
    for r in results:
        print(f"{r['platform']:<18} | {r['metric']:<38} | {r['value']}")
    print(f"\nSaved to {FOOTPRINT_FILE}")


if __name__ == "__main__":
    main()