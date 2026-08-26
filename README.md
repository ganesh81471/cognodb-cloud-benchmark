# CognoDB Cloud Graph Database Benchmark

An in-progress, reproducible benchmark of CognoDB Cloud and four self-hosted graph databases: Neo4j, Memgraph, FalkorDB, and ArangoDB. It aims for an honest comparison on one dataset and equivalent logical workloads; it does not claim a universal winner.

## Status

Completed: dataset preparation, ingestion, lookups, and aggregations. The traversal benchmark is currently being rerun after introducing a uniform 3-hop bound. The mixed concurrent read/write workload and footprint collection are still pending, so the repository is not submission-complete.

## Platforms and resources

| Platform | Deployment | Interface | Resource configuration |
|---|---|---|---|
| CognoDB Cloud | Managed `c0` free tier | Cypher/Bolt | 0.5 vCPU, 256 MB RAM, 1 GB disk |
| Neo4j | Local Docker | Cypher/Bolt | 0.5 CPU, 256 MB RAM; heap 128 MB; page cache 64 MB |
| Memgraph | Local Docker | Cypher/Bolt | 0.5 CPU, 256 MB RAM; internal limit 180 MB |
| FalkorDB | Local Docker | Cypher/Redis | 0.5 CPU, 256 MB RAM |
| ArangoDB | Local Docker | AQL | 0.5 CPU, 256 MB RAM |

Docker resource caps approximate CognoDB's free tier. Memgraph's 180 MB internal limit is below the container cap and is disclosed rather than compensated for with more memory.

CognoDB is accessed remotely; the other platforms run locally on the benchmark client. Thus CognoDB latencies include network round-trip time, while local latencies do not. Treat this topology difference as a material caveat.

## Dataset and schema

Source: [MovieLens Latest Small](https://files.grouplens.org/datasets/movielens/ml-latest-small.zip) from GroupLens Research. `download_data.py` downloads it; raw data is gitignored.

| Element | Count |
|---|---:|
| Users | 610 |
| Movies | 9,742 |
| Genres | 19 |
| **Nodes** | **10,371** |
| `RATED` relationships | 100,836 |
| `HAS_GENRE` relationships | 22,050 |
| **Relationships** | **122,886** |

```text
(:User {userId})-[:RATED {rating, timestamp}]->(:Movie {movieId, title})
(:Movie)-[:HAS_GENRE]->(:Genre {name})
```

Indexes are created for `User.userId`, `Movie.movieId`, and `Genre.name`.

## Methodology

- Same MovieLens graph and logical schema on every platform.
- Read workloads use 10 warm-up runs and 100 measured runs where indicated.
- Results report p50 and p95 milliseconds; raw JSON is retained in `results/`.
- Aggregation user inputs use one deterministic sequence across all platforms.
- The 3-hop traversal limits the deduplicated co-rater set to 50 users before its final expansion. The same deterministic bound is used in Cypher, FalkorDB, and AQL to fit the smallest resource envelope.
- Failed runs and timeouts are reported, not hidden.

## Ingestion results

All loaders recorded 10,371 nodes and 122,886 relationships.

| Platform | Load time (s) | Nodes/s | Relationships/s | Load method |
|---|---:|---:|---:|---|
| CognoDB Cloud | 61.48 | 168.69 | 1,998.78 | Cypher `UNWIND`, Bolt |
| Local Neo4j | 92.98 | 111.54 | 1,321.61 | Cypher `UNWIND`, Bolt |
| Local Memgraph | 9.69 | 1,070.16 | 12,680.37 | Cypher `UNWIND`, Bolt |
| Local FalkorDB | 15.36 | 675.12 | 7,999.45 | Cypher `UNWIND`, `GRAPH.QUERY` |
| Local ArangoDB | 11.77 | 880.94 | 10,438.28 | Native `insert_many()` |

ArangoDB uses its native bulk API and timed setup work differs between loaders. These are end-to-end loader measurements, not identical import mechanics.

## Lookup results (provisional)

The current file contains 100 measured executions per workload. Point lookup uses indexed `User.userId`; `Filtered_Lookup` filters unindexed `Movie.title`, so it is a scan/filter workload. Bolt warm-up results must be consumed before final submission, therefore these numbers are provisional.

| Platform | Point p50 / p95 (ms) | Title-filter p50 / p95 (ms) |
|---|---:|---:|
| CognoDB Cloud | 409.02 / 613.97 | 408.59 / 613.93 |
| Local Neo4j | 10.46 / 87.85 | 20.08 / 145.52 |
| Local Memgraph | 0.78 / 1.58 | 3.86 / 53.76 |
| Local FalkorDB | 0.54 / 2.26 | 1.66 / 2.46 |
| Local ArangoDB | 48.09 / 51.53 | 52.05 / 54.76 |

## Aggregation results

- `Global_Avg_Rating`: average `RATED.rating` over the graph.
- `Rating_Distribution`: count/group-by of `RATED.rating` values.
- `User_Genre_Agg`: average rating grouped by genre for a sampled user's rated movies.

All 15 aggregation results completed with 100% success.

| Platform | Global avg p50 / p95 (ms) | Rating distribution p50 / p95 (ms) | User/genre p50 / p95 (ms) |
|---|---:|---:|---:|
| CognoDB Cloud | 998.38 / 1,278.26 | 1,023.03 / 1,229.58 | 409.33 / 729.79 |
| Local Neo4j | 520.75 / 733.90 | 502.45 / 677.62 | 18.70 / 116.14 |
| Local Memgraph | 26.74 / 82.42 | 51.95 / 81.78 | 1.78 / 4.67 |
| Local FalkorDB | 118.23 / 173.98 | 119.93 / 179.09 | 1.90 / 4.45 |
| Local ArangoDB | 70.58 / 82.64 | 72.90 / 92.10 | 52.35 / 65.63 |

The earlier top-movie group-by exceeded Memgraph's 180 MB internal limit. It was replaced uniformly with `Rating_Distribution`; this is a documented workload definition, not a platform-specific optimization.

## Traversals and mixed workload

`workloads/traversals.py` measures 1-hop, 2-hop, and bounded 3-hop latency. The traversal rerun is in progress; do not treat older traversal JSON as final until it has valid entries for every platform.

The required concurrent mixed read/write throughput workload and observable storage/memory footprint collection are not yet implemented.

## Reproduce

Requirements: Python 3, Docker Desktop, and CognoDB credentials. Store secrets only in a local `.env` file; never commit it.

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python download_data.py
# Start the Docker containers with the resource settings above.
python load_data.py
python workloads\lookups.py
python workloads\traversals.py
python workloads\aggregations.py
```

Required variables: `COGNODB_URI`, `COGNODB_USER`, `COGNODB_PASSWORD`, `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`, `MEMGRAPH_URI`, `MEMGRAPH_USER`, `MEMGRAPH_PASSWORD`, `FALKORDB_HOST`, `FALKORDB_PORT`, `ARANGODB_URL`, `ARANGODB_USER`, and `ARANGODB_PASSWORD`.

## Repository layout

```text
download_data.py                 # Fetch and summarize MovieLens
load_data.py                     # Load the common graph into all platforms
workloads/lookups.py             # Point and filtered lookups
workloads/traversals.py          # 1-, 2-, and bounded 3-hop traversals
workloads/aggregations.py        # Global and group-by aggregations
results/*.json                   # Raw benchmark results
```

## Remaining work

1. Finish and validate the traversal rerun.
2. Consume lookup warm-up results and rerun lookups.
3. Implement mixed concurrent read/write throughput.
4. Collect observable footprint metrics and deployment details.
5. Add charts and final analysis.
