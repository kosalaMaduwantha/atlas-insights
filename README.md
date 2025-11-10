Atlas Insights – Data Ingestion & Big Data Analytics Framework
=======================================================

Comprehensive, metadata‑driven ingestion framework for big data analytics.:

Currently Data Ingestion module is implemented for three primary source types (Downstream applications such as Reporting and recommendation services are yet to be implemented):

* File system (CSV ➜ ORC format on HDFS)
* Relational databases (MySQL / PostgreSQL / SQL Server ➜ ORC format on HDFS)
* Streaming (WebSocket JSON ➜ Kafka topics – foundation for near‑real‑time ingestion)
* Load data(ingested ORC) into Hive external tables.

Built with Python, PyArrow, FastAPI, and Kafka; designed to standardize how raw operational data is landed into an analytics‑friendly lake/warehouse zone (HDFS ORC format) using a **single metadata contract**.

---
## 🗺️ High-Level Architecture

![Atlas Insights High-Level Architecture](<docs/Atlas Insights-High Level Architecture(in progress).drawio.svg>)

See `docs/architecture.md` for a deeper component breakdown.

---
## ✨ Key Features

* **Metadata‑Driven**: A JSON file (one per logical ingestion group – "ocs group") defines sources, schema (features), destination path, and transformation placeholder. Future: Separate metadata service will be integrated.
* **Unified Schema Builder**: `build_schema()` maps declared logical dtypes to PyArrow schema.
* **Pluggable Sources**: File system & RDBMS implemented; streaming scaffold present.
* **Batch Efficiency**: RDBMS ingestion streams fetches (`fetch_many`) with configurable fetch size.
* **HDFS Optimized Read**: ORC format is optimized for read performance in HDFS.
* **Stateless Execution**: Each ingestion run is deterministic based on metadata JSON.
* **Extensible**: Add new source types (API, object storage) or sinks (Delta Lake, Iceberg) with minimal coupling.

## Overview of the system

This Data ingestion framework is design to ingest data from variable types of sources to HDFS file system. As mentioned above currently the ingestion is implemented for File system type (CSV), Relational database systems and Streaming data via KAFKA.

### File system type

Currently, the supported file type is **CSV**. In the future, more file types will be implemented.

The following is the flow of CSV ingestion:

1. Read metadata from the local filesystem (for CSV-based ingestion, the metadata file is named `ocs_group_fs.json`).
    >💡 **Note**: To configure a CSV-based data source, the metadata file name should follow the format: `ocs_group_name_fs`.
2. Read the set of files defined in the metadata configuration.
3. Filter only the columns that need to be ingested.
4. Convert the filtered dataset into **ORC** format.
5. Load the data into the HDFS filesystem (the HDFS path needs to be configured in the metadata).

### RDBMS Ingestion

Currently supports postgresql, mysql/mariadb and mssql RDBMS types.

The following is the flow of RDBMS ingestion

1. Read metadata from the local filesystem (for RDBMS based ingestion, the metadata file is named `ocs_group_rdbms.json`).
    >💡 **Note**: To configure a RDBMS based data source, the metadata file name should follow the format: `ocs_group_rdbms`.
2. Read the set of tables defined in the metadata with the filter columns.
3. Convert the filtered dataset into **ORC** format.
4. Load the data into the HDFS filesystem (the HDFS path needs to be configured in the metadata).

### Streaming via KAFKA

A WebSocket-based API has been created to publish events to a specific topic (which can be passed as a query parameter), and a subscriber pipeline has been established from a Kafka broker to the HDFS file system.

The following is the flow of the KAFKA streaming pipeline

1. Publisher application(Data source) sends an event through the web socket API.
2. Back-end validates the data coming from the publisher application(Uses the metadata configuration).
3. Once validated backend publishes the event to the respective KAFKA topic mentioned in the query parameter.
4. A subscriber application listens to the KAFKA topic and writes the data to the HDFS file system in ORC format(batch size can be configured. if the messages up to the batch size is accumulated the subscriber application will write them to HDFS in ORC format).

## 🧾 Metadata Specification (Summary)

Each metadata file adheres to this high-level schema (see `docs/metadata_schema.md` for full detail):

```jsonc
{
	"ocs_group_name": "ecommerce_transactions_fs",
	"source_type": "fs|rdbms|streaming",
	"source_config": { /* connection or file format info */ },
	"dataset_config": [
		{
			"source": {
				"name": "logical_dataset_name",
				"path": "filesystem path | table name | topic/stream id",
				"features": [ {"name": "ColumnA", "dtype": "int"}, ... ]
			},
			"transformations": [],
			"destination": {
				"path": "/data/warehouse/...",
				"features": [ /* optional projected schema (mirrors source) */ ]
			}
		}
	]
}
```

Support dtypes (current mapping): `int|integer|bigint|float|double|string|text|datetime|timestamp`. (Note: `date` appears in one metadata file but is currently not mapped; see "Known Gaps".)

---
## 🚀 Quick Start

### 1. Clone & Environment

```bash
git clone <repo-url> atlas-insights
cd atlas-insights
python -m venv env
source env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Prepare HDFS (example)

Ensure an HDFS namenode is reachable (default host `localhost`, port `9000`). Adjust via env vars:

```bash
export HDFS_HOST=my-hdfs-host
export HDFS_PORT=9870   # if using a different port
```

### 3. Run CSV Ingestion

```bash
python -m src.ingestion.csv_ingestion --ocs ecommerce_transactions_fs
```

This will read the CSV at the `path` defined in the metadata and write Parquet to the configured HDFS destination path.

### 4. Run RDBMS Ingestion (MySQL example)

Ensure MySQL is running and credentials match `ecommerce_transactions_rdbms.json`.

```bash
python -m src.ingestion.rdbms_ingestion --ocs ecommerce_transactions_rdbms --fetch-size 5000
```

### 5. Start Kafka Publisher API (WebSocket)

```bash
uvicorn src.kafka_api_pub.publisher_api:app --host 0.0.0.0 --port 8000 --reload
```

Then connect a WebSocket client (e.g. browser / `websocat`) to:

`ws://localhost:8000/ws?ocs_group=ecommerce_transactions_streaming`

Send JSON messages; server validates against features list and publishes to Kafka topic named after `ocs_group`.

### 6. Kafka Topic & Broker

Broker assumed at `localhost:9092`. To override, update `publish_to_kafka()` or refactor to read an env var (see Future Enhancements).

---
## 🛠️ CLI Usage Reference

| Command | Description | Key Options |
|---------|-------------|-------------|
| `python -m src.ingestion.csv_ingestion` | File system CSV ➜ Parquet | `--ocs <metadata base name>` |
| `python -m src.ingestion.rdbms_ingestion` | RDBMS table ➜ Parquet | `--ocs`, `--fetch-size` |
| `uvicorn src.kafka_api_pub.publisher_api:app` | Start WebSocket Kafka publisher | `--port` |

---
## ⚙️ Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `HDFS_HOST` | `localhost` | HDFS Namenode host |
| `HDFS_PORT` | `9000` | HDFS port (int) |

Add to shell or `.env` (if integrating python-dotenv for auto‑loading).

---
## 📂 Data Flow Details

### CSV Ingestion
1. Load metadata (`load_metadata`).
2. Build schema from `features`.
3. Read entire CSV via `pyarrow.csv.read_csv`.
4. Column subset enforced by feature list order.
5. Rows transformed into list[dict] → passed as a single batch to Parquet writer.

### RDBMS Ingestion
1. Connect using driver chosen by `db_type`.
2. Stream rows in `fetch_size` batches.
3. Convert each batch into list[dict]; feed to common Parquet writer.
4. Single output Parquet file per dataset per run.

### Streaming (Current State)
* WebSocket API validates incoming JSON fields exist.
* Publishes to Kafka; no persistence pipeline yet from Kafka ➜ HDFS (future enhancement).

### ORC Writing
* Reorders row dicts to match schema field order.
* Writes one consolidated file: `<destination_path>/<dataset_name>.orc`.
* Creates HDFS directory if absent.

---
## 🧪 Testing Strategy

Pending implementation. Recommended layers:
* Unit: schema mapping, metadata loading, identifier quoting per DB.
* Integration: mock DB cursor streaming; temporary local filesystem instead of HDFS (use `pyarrow.fs.LocalFileSystem()`).
* Contract: validate metadata JSON against JSONSchema.

---
## 🔧 Extending the Framework



---
## 🐛 Known Gaps / TODO

| Area | Detail | Suggested Fix |
|------|--------|---------------|
| Data Type Mapping | `date` used in `ecommerce_transactions_fs.json` not mapped | Add `'date': pa.date32()` to `_PYARROW_TYPE_MAP` |
| Error Handling | Minimal retry / backoff for DB & Kafka | Introduce retry wrapper (e.g. tenacity) |
| Idempotency | Overwrites Parquet file each run | Add run timestamp or partition folders (`run_date=...`) |
| Streaming Persistence | Kafka → HDFS path missing | Add consumer job & scheduler / streaming ingestion module |
| Configuration | Hardcoded Kafka bootstrap servers | Externalize via env / metadata |
| Security | Plaintext DB credentials in JSON | Support secret manager / env interpolation |

---
## 📊 Observability (Future)

Recommend adding:
* Structured logging (JSON) & correlation IDs.
* Basic metrics: rows processed, duration, throughput.
* Optional OpenTelemetry instrumentation for tracing.

---
## 🔐 Security Considerations
* Avoid committing real credentials in metadata.
* Restrict filesystem paths to approved directories.
* Validate and sanitize all streaming JSON input (length limits, type enforcement).

---
## 🗃️ Versioning & Data Lifecycle
Currently single overwrite semantics. To enable time travel or retention:
1. Write to dated folders: `/data/warehouse/<dataset>/ingest_date=YYYY-MM-DD/part-*.orc`.
2. Introduce a manifest table or Hive/Trino metastore integration.

## 📚 Additional Documentation
See:
* `docs/architecture.md`
* `docs/metadata_schema.md`

---
## 🙌 Acknowledgements
Powered by PyArrow, FastAPI, kafka-python, and the broader open-source ecosystem.

