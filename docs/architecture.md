# Architecture Deep Dive

## High-Level Architecture

The following diagram presents the high-level architecture of Atlas Insights, showing major data flows and components before diving into detailed component responsibilities below.

![Atlas Insights High-Level Architecture](<Atlas Insights-High Level Architecture(in progress).drawio.svg>)

<sub>Source: draw.io diagram stored alongside this document. Update the `.drawio.svg` file to revise the diagram; no Markdown changes needed unless the filename changes.</sub>

## Component Overview

| Component | File | Responsibility |
|-----------|------|----------------|
| Metadata Config | `src/config/*.json` | Declarative source + destination + schema (features) per ocs group |
| CSV Ingestion | `src/ingestion/csv_ingestion.py` | Reads CSV via PyArrow, filters columns, writes Parquet to HDFS |
| RDBMS Ingestion | `src/ingestion/rdbms_ingestion.py` | Streams table rows from supported RDBMS into Parquet |
| Streaming Publisher API | `src/kafka_api_pub/publisher_api.py` | WebSocket endpoint to validate JSON and publish to Kafka |
| HDFS Writer | `src/providers/hdfs_service.py` | Unified Parquet write abstraction for any batch source |
| RDBMS Service | `src/providers/rdbms_service.py` | Connection factory + batch fetch generator |
| Schema Utilities | `src/utils/common_util_func.py` | Schema building + metadata loader |

## Data Flow (Batch)

1. User executes ingestion module with `--ocs <group>`.
2. Module loads `src/config/<group>.json` using `load_metadata()`.
3. For each `dataset_config` entry:
   * Source definition extracted (path, features).
   * Destination path resolved.
   * Schema generated with `build_schema(features)`.
4. Source reader produces in-memory batches of `List[Dict]` rows.
5. `write_parquet_dataset()` converts row batches → Arrow tables → single Parquet file in HDFS.

## Data Flow (Streaming – Current Slice)

```
Client WS -> FastAPI /ws -> validate_data() -> publish_to_kafka() -> Kafka Topic
```

Persistence from Kafka → HDFS not yet implemented (planned consumer job).

## Batch Size Strategy

RDBMS ingestion uses `fetch_size` controlling `cursor.fetchmany()` to balance memory vs. throughput. CSV path currently loads full file (can be optimized to chunked streaming if large). 

## Schema Handling

`build_schema()` maps supported logical types to Arrow physical types:

| Logical | Arrow Type |
|---------|------------|
| int, integer, bigint | `pa.int64()` |
| float, double | `pa.float64()` |
| string, text | `pa.string()` |
| datetime, timestamp | `pa.timestamp('s')` |

(Planned) Add `date -> pa.date32()`.

## Error Handling Philosophy

* Early validation: destination path presence.
* Fail-fast on unsupported DB types.
* Suppresses non-critical HDFS directory existence errors.
* Future: structured exceptions + retry/backoff utilities.

## Extensibility Points

* Add new ingestion: implement module that yields `Iterable[List[Dict]]` and reuse writer.
* Alternate sink: create new provider (e.g. S3, ADLS) using Arrow Filesystem API.
* Transformations: inject a pre-write pipeline operating on row dicts or Arrow Table.
* Partitioning: migrate from single Parquet file to `pq.write_to_dataset()` for partition columns.

## Operational Concerns

| Concern | Current State | Improvement |
|---------|---------------|------------|
| Idempotency | Overwrites output file | Add timestamped folders or transactional staging |
| Observability | Basic logging | Structured logs + metrics + tracing |
| Security | Plaintext creds in JSON | External secret store / env interpolation |
| Schema Evolution | Manual replacement | Versioned metadata, migration detection |
| Scaling | Single-process ingestion | Parallel partition writing / multi-worker orchestration |

## Suggested Consumer Pattern (Future)

```
Kafka Consumer -> Deserialize -> Validate -> Micro-batching -> write_parquet_dataset() -> HDFS (partitioned by event_date)
```

## Sequence Diagram (RDBMS)

```
User -> rdbms_ingestion.py: run --ocs group
rdbms_ingestion.py -> common_util_func: load_metadata()
rdbms_ingestion.py -> rdbms_service: connect_db()
loop datasets
  rdbms_ingestion.py -> rdbms_service: fetch_batches()
  rdbms_ingestion.py -> hdfs_service: write_parquet_dataset(batches)
  hdfs_service -> HDFS: create_dir + write parquet
end
rdbms_ingestion.py -> DB: close connection
```

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Large CSV memory load | OOM | Switch to streaming CSV reading with row groups |
| Partial failure mid-write | Corrupted file | Write to temp path then atomic rename |
| Schema drift | Downstream breakage | Add schema registry / validation gate |
| Slow DB extraction | SLA miss | Incremental ingestion via watermark columns |
| Kafka backpressure (future) | Lag growth | Use consumer groups + partitioning |

## Technology Rationale

* PyArrow: columnar speed + unified filesystem abstraction.
* FastAPI: async-friendly WebSocket & lightweight API surface.
* kafka-python: simple producer bindings (can swap to confluent-kafka for performance).
* Metadata JSON: human-readable, git-versioned contract.

## Roadmap Linkage

See root README for chronological roadmap; this doc maps each item to architecture blocks for traceability.
