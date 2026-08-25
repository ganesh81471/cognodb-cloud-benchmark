"""
===============================================================================
                         MASTER BENCHMARK SCHEMA CONTRACT
===============================================================================
Node Labels & Properties:
  - :User   { userId: Integer }
  - :Movie  { movieId: Integer, title: String }
  - :Genre  { name: String }

Relationships & Properties:
  - (:User)-[:RATED { rating: Float, timestamp: Integer }]->(:Movie)
  - (:Movie)-[:HAS_GENRE]->(:Genre)

Indexes Created:
  - User(userId)
  - Movie(movieId)
  - Genre(name)

LOAD METHOD NOTES (document these in README Section 5/10):
  - CognoDB / Neo4j / Memgraph: loaded via Cypher UNWIND batches (batch_size=1000)
    over the Bolt driver.
  - FalkorDB: loaded via Cypher UNWIND batches (batch_size=1000) over
    GRAPH.QUERY, matching the other Cypher platforms so load method is
    consistent across all Cypher-speaking targets.
  - ArangoDB: loaded via python-arango's insert_many() bulk API
    (batch_size=2000), since ArangoDB has no Cypher/UNWIND equivalent —
    this is its native bulk-insert mechanism.
===============================================================================
"""

import os
import json
import time
import pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase
from arango import ArangoClient
import redis

load_dotenv()

DATA_DIR = os.path.join("dataset", "ml-latest-small")
RATINGS_PATH = os.path.join(DATA_DIR, "ratings.csv")
MOVIES_PATH = os.path.join(DATA_DIR, "movies.csv")

RESULTS_PATH = os.path.join("results", "ingestion_results.json")


# -----------------------------------------------------------------------------
# 0. RESULTS LOGGING
# -----------------------------------------------------------------------------
def log_result(platform, total_time, total_nodes, total_edges, load_method):
    os.makedirs("results", exist_ok=True)

    entry = {
        "platform": platform,
        "total_time_sec": round(total_time, 2),
        "total_nodes": total_nodes,
        "total_edges": total_edges,
        "node_rate_per_sec": round(total_nodes / total_time, 2) if total_time > 0 else None,
        "edge_rate_per_sec": round(total_edges / total_time, 2) if total_time > 0 else None,
        "load_method": load_method,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    existing = []
    if os.path.exists(RESULTS_PATH):
        with open(RESULTS_PATH, "r") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = []

    # Replace any previous entry for this platform so reruns don't duplicate
    existing = [e for e in existing if e["platform"] != platform]
    existing.append(entry)

    with open(RESULTS_PATH, "w") as f:
        json.dump(existing, f, indent=2)

    print(f"Logged results for {platform} -> {RESULTS_PATH}")


# -----------------------------------------------------------------------------
# 1. CYPHER LOADER (CognoDB Cloud, Local Neo4j, Local Memgraph)
# -----------------------------------------------------------------------------
def load_cypher_db(name, uri, user, password, movies_df, ratings_df, batch_size=1000):
    print(f"\n==========================================")
    print(f" Loading Data into: {name} (Cypher)")
    print(f"==========================================")

    if not uri:
        print(f"Skipping {name}: URI not configured in .env")
        return

    auth = (user, password) if user or password else None
    driver = GraphDatabase.driver(uri, auth=auth)

    start_time = time.time()

    with driver.session() as session:
        print("Cleaning old data...")
        session.run("MATCH (n) DETACH DELETE n")

        print("Creating indexes per Schema Contract...")
        try:
            session.run("CREATE INDEX FOR (u:User) ON (u.userId)")
            session.run("CREATE INDEX FOR (m:Movie) ON (m.movieId)")
            session.run("CREATE INDEX FOR (g:Genre) ON (g.name)")
        except Exception as idx_err:
            print(f"Index setup notice: {idx_err}")

        # Prepare Nodes
        user_ids = [{"userId": int(u)} for u in ratings_df['userId'].unique()]
        movie_nodes = []
        genre_set = set()
        genre_edges = []

        for _, row in movies_df.iterrows():
            m_id = int(row['movieId'])
            movie_nodes.append({"movieId": m_id, "title": str(row['title'])})
            if pd.notna(row['genres']) and row['genres'] != "(no genres listed)":
                for g in row['genres'].split('|'):
                    genre_set.add(g)
                    genre_edges.append({"movieId": m_id, "genre": g})

        genre_nodes = [{"name": g} for g in genre_set]

        # Ingest Nodes
        print(f"Ingesting {len(user_ids)} Users, {len(movie_nodes)} Movies, {len(genre_nodes)} Genres...")
        session.run("UNWIND $batch AS row CREATE (:User {userId: row.userId})", batch=user_ids)
        session.run("UNWIND $batch AS row CREATE (:Movie {movieId: row.movieId, title: row.title})", batch=movie_nodes)
        session.run("UNWIND $batch AS row CREATE (:Genre {name: row.name})", batch=genre_nodes)

        # Ingest HAS_GENRE Relationships
        print(f"Ingesting {len(genre_edges)} HAS_GENRE relationships...")
        for i in range(0, len(genre_edges), batch_size):
            batch = genre_edges[i:i + batch_size]
            session.run("""
                UNWIND $batch AS row
                MATCH (m:Movie {movieId: row.movieId})
                MATCH (g:Genre {name: row.genre})
                CREATE (m)-[:HAS_GENRE]->(g)
            """, batch=batch)

        # Ingest RATED Relationships
        rating_edges = [
            {"userId": int(r.userId), "movieId": int(r.movieId), "rating": float(r.rating), "timestamp": int(r.timestamp)}
            for r in ratings_df.itertuples()
        ]

        total_ratings = len(rating_edges)
        print(f"Ingesting {total_ratings} RATED relationships...")
        for i in range(0, total_ratings, batch_size):
            batch = rating_edges[i:i + batch_size]
            session.run("""
                UNWIND $batch AS row
                MATCH (u:User {userId: row.userId})
                MATCH (m:Movie {movieId: row.movieId})
                CREATE (u)-[:RATED {rating: row.rating, timestamp: row.timestamp}]->(m)
            """, batch=batch)

    total_time = time.time() - start_time
    total_nodes = len(user_ids) + len(movie_nodes) + len(genre_nodes)
    total_edges = len(genre_edges) + total_ratings

    print(f"\n--- {name} Results ---")
    print(f"Total Time: {total_time:.2f} seconds")
    print(f"Node Rate:  {total_nodes / total_time:.2f} nodes/sec")
    print(f"Edge Rate:  {total_edges / total_time:.2f} edges/sec")

    log_result(name, total_time, total_nodes, total_edges, load_method="Cypher UNWIND batches (batch_size=1000) via Bolt driver")

    driver.close()


# -----------------------------------------------------------------------------
# 2. FALKORDB LOADER (Cypher over Redis protocol, UNWIND-batched to match
#    the other Cypher platforms' load method)
# -----------------------------------------------------------------------------
def load_falkordb(host, port, movies_df, ratings_df, batch_size=1000):
    print(f"\n==========================================")
    print(f" Loading Data into: Local FalkorDB (Cypher over Redis)")
    print(f"==========================================")

    r = redis.Redis(host=host, port=int(port))
    graph_name = "movielens"

    # Delete previous graph
    try:
        r.execute_command("GRAPH.DELETE", graph_name)
    except Exception:
        pass

    start_time = time.time()

    # Prepare Data
    user_ids = ratings_df['userId'].unique()
    genre_set = set()
    genre_edges = []
    movie_nodes = []

    for _, row in movies_df.iterrows():
        m_id = int(row['movieId'])
        movie_nodes.append({"movieId": m_id, "title": str(row['title'])})
        if pd.notna(row['genres']) and row['genres'] != "(no genres listed)":
            for g in row['genres'].split('|'):
                genre_set.add(g)
                genre_edges.append({"movieId": m_id, "genre": g})

    genre_nodes = [{"name": g} for g in genre_set]
    user_nodes = [{"userId": int(u)} for u in user_ids]

    print(f"Creating indexes per Schema Contract...")
    try:
        r.execute_command("GRAPH.QUERY", graph_name, "CREATE INDEX FOR (u:User) ON (u.userId)")
        r.execute_command("GRAPH.QUERY", graph_name, "CREATE INDEX FOR (m:Movie) ON (m.movieId)")
        r.execute_command("GRAPH.QUERY", graph_name, "CREATE INDEX FOR (g:Genre) ON (g.name)")
    except Exception as idx_err:
        print(f"Index setup notice: {idx_err}")

    print(f"Ingesting {len(user_nodes)} Users, {len(movie_nodes)} Movies, {len(genre_nodes)} Genres...")


    # FalkorDB's GRAPH.QUERY doesn't support the same $param binding as the
    # Bolt driver, so params are inlined into the query text per batch.
    def to_cypher_value(v):
        if isinstance(v, str):
            escaped = v.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"
        elif v is None:
            return "null"
        else:
            return str(v)
        
    def to_cypher_map(d):
        fields = ", ".join(f"{k}: {to_cypher_value(v)}" for k, v in d.items())
        return "{" + fields + "}"

    def to_cypher_list_of_maps(rows):
        return "[" + ", ".join(to_cypher_map(r) for r in rows) + "]"

    def unwind_create(label_query_fn, rows, size=batch_size):
        for i in range(0, len(rows), size):
            batch = rows[i:i + size]
            batch_literal = to_cypher_list_of_maps(batch)
            query = f"UNWIND {batch_literal} AS row {label_query_fn}"
            r.execute_command("GRAPH.QUERY", graph_name, query)
            
    unwind_create("CREATE (:User {userId: row.userId})", user_nodes)
    unwind_create("CREATE (:Movie {movieId: row.movieId, title: row.title})", movie_nodes)
    unwind_create("CREATE (:Genre {name: row.name})", genre_nodes)

    print(f"Ingesting {len(genre_edges)} HAS_GENRE relationships...")
    unwind_create(
        "MATCH (m:Movie {movieId: row.movieId}) MATCH (g:Genre {name: row.name}) CREATE (m)-[:HAS_GENRE]->(g)",
        [{"movieId": e["movieId"], "name": e["genre"]} for e in genre_edges]
    )

    rating_edges = [
        {"userId": int(r_.userId), "movieId": int(r_.movieId), "rating": float(r_.rating), "timestamp": int(r_.timestamp)}
        for r_ in ratings_df.itertuples()
    ]
    print(f"Ingesting {len(rating_edges)} RATED relationships...")
    unwind_create(
        "MATCH (u:User {userId: row.userId}) MATCH (m:Movie {movieId: row.movieId}) "
        "CREATE (u)-[:RATED {rating: row.rating, timestamp: row.timestamp}]->(m)",
        rating_edges
    )

    total_time = time.time() - start_time
    total_nodes = len(user_nodes) + len(movie_nodes) + len(genre_nodes)
    total_edges = len(genre_edges) + len(rating_edges)

    print(f"\n--- Local FalkorDB Results ---")
    print(f"Total Time: {total_time:.2f} seconds")
    print(f"Node Rate:  {total_nodes / total_time:.2f} nodes/sec")
    print(f"Edge Rate:  {total_edges / total_time:.2f} edges/sec")

    log_result("Local FalkorDB", total_time, total_nodes, total_edges, load_method="Cypher UNWIND batches (batch_size=1000) via GRAPH.QUERY")


# -----------------------------------------------------------------------------
# 3. ARANGODB LOADER (Document / Graph Model via python-arango bulk insert)
# -----------------------------------------------------------------------------
def load_arangodb(url, user, password, movies_df, ratings_df, batch_size=2000):
    print(f"\n==========================================")
    print(f" Loading Data into: Local ArangoDB")
    print(f"==========================================")

    client = ArangoClient(hosts=url)
    sys_db = client.db('_system', username=user, password=password)

    db_name = "movielens_db"
    if sys_db.has_database(db_name):
        sys_db.delete_database(db_name)
    sys_db.create_database(db_name)

    db = client.db(db_name, username=user, password=password)

    start_time = time.time()

    # Create Collections
    users_coll = db.create_collection('User')
    movies_coll = db.create_collection('Movie')
    genres_coll = db.create_collection('Genre')

    rated_coll = db.create_collection('RATED', edge=True)
    has_genre_coll = db.create_collection('HAS_GENRE', edge=True)

    # Indexes
    users_coll.add_index({'type': 'persistent', 'fields': ['userId'], 'unique': True})
    movies_coll.add_index({'type': 'persistent', 'fields': ['movieId'], 'unique': True})
    genres_coll.add_index({'type': 'persistent', 'fields': ['name'], 'unique': True})

    # Prepare Data
    user_docs = [{"_key": str(u), "userId": int(u)} for u in ratings_df['userId'].unique()]

    movie_docs = []
    genre_set = set()
    genre_edges = []

    for _, row in movies_df.iterrows():
        m_id = int(row['movieId'])
        movie_docs.append({"_key": str(m_id), "movieId": m_id, "title": str(row['title'])})
        if pd.notna(row['genres']) and row['genres'] != "(no genres listed)":
            for g in row['genres'].split('|'):
                genre_set.add(g)
                genre_edges.append({"_from": f"Movie/{m_id}", "_to": f"Genre/{g}"})

    genre_docs = [{"_key": g, "name": g} for g in genre_set]

    print(f"Ingesting {len(user_docs)} Users, {len(movie_docs)} Movies, {len(genre_docs)} Genres...")
    users_coll.insert_many(user_docs)
    movies_coll.insert_many(movie_docs)
    genres_coll.insert_many(genre_docs)

    print(f"Ingesting {len(genre_edges)} HAS_GENRE relationships...")
    # FIX: HAS_GENRE edges now go into has_genre_coll (was already correct here)
    for i in range(0, len(genre_edges), batch_size):
        has_genre_coll.insert_many(genre_edges[i:i + batch_size])

    rating_edges = [
        {"_from": f"User/{int(r.userId)}", "_to": f"Movie/{int(r.movieId)}", "rating": float(r.rating), "timestamp": int(r.timestamp)}
        for r in ratings_df.itertuples()
    ]

    print(f"Ingesting {len(rating_edges)} RATED relationships...")
    # FIX: this loop was previously inserting into has_genre_coll by mistake.
    # RATED edges must go into rated_coll, not has_genre_coll.
    for i in range(0, len(rating_edges), batch_size):
        rated_coll.insert_many(rating_edges[i:i + batch_size])

    total_time = time.time() - start_time
    total_nodes = len(user_docs) + len(movie_docs) + len(genre_docs)
    total_edges = len(genre_edges) + len(rating_edges)

    print(f"\n--- Local ArangoDB Results ---")
    print(f"Total Time: {total_time:.2f} seconds")
    print(f"Node Rate:  {total_nodes / total_time:.2f} nodes/sec")
    print(f"Edge Rate:  {total_edges / total_time:.2f} edges/sec")

    log_result("Local ArangoDB", total_time, total_nodes, total_edges, load_method="python-arango insert_many() bulk API (batch_size=2000)")


# -----------------------------------------------------------------------------
# MAIN EXECUTION
# -----------------------------------------------------------------------------
def main():
    movies_df = pd.read_csv(MOVIES_PATH)
    ratings_df = pd.read_csv(RATINGS_PATH)

    # 1. Cypher Targets
    cypher_targets = {
        "CognoDB Cloud": (os.getenv("COGNODB_URI"), os.getenv("COGNODB_USER"), os.getenv("COGNODB_PASSWORD")),
        "Local Neo4j": (os.getenv("NEO4J_URI"), os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD")),
        "Local Memgraph": (os.getenv("MEMGRAPH_URI"), os.getenv("MEMGRAPH_USER") or "", os.getenv("MEMGRAPH_PASSWORD") or "")
    }

    for name, (uri, user, password) in cypher_targets.items():
        try:
            load_cypher_db(name, uri, user, password, movies_df, ratings_df)
        except Exception as e:
            print(f"Failed {name}: {e}")

    # 2. FalkorDB
    try:
        load_falkordb(os.getenv("FALKORDB_HOST", "localhost"), os.getenv("FALKORDB_PORT", 6379), movies_df, ratings_df)
    except Exception as e:
        print(f"Failed FalkorDB: {e}")

    # 3. ArangoDB
    try:
        load_arangodb(
            os.getenv("ARANGODB_URL", "http://localhost:8529"),
            os.getenv("ARANGODB_USER", "root"),
            os.getenv("ARANGODB_PASSWORD", "password123"),
            movies_df, ratings_df
        )
    except Exception as e:
        print(f"Failed ArangoDB: {e}")


if __name__ == "__main__":
    main()