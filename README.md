Atlas Insights – Data Ingestion & Streaming Microservice
=======================================================

Comprehensive, metadata‑driven ingestion framework for big data analytics.:

Currently supports three primary ingestion patterns:

* File system (CSV ➜ Parquet on HDFS)
* Relational databases (MySQL / PostgreSQL / SQL Server ➜ Parquet on HDFS)
* Streaming (WebSocket JSON ➜ Kafka topics – foundation for near‑real‑time ingestion)

Built with Python, PyArrow, FastAPI, and Kafka; designed to standardize how raw operational data is landed into an analytics‑friendly lake/warehouse zone (HDFS Parquet) using a **single metadata contract**.

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
* **HDFS Optimized Write**: Single Parquet writer per dataset; appends row batches sequentially.
* **Stateless Execution**: Each ingestion run is deterministic based on metadata JSON.
* **Extensible**: Add new source types (API, object storage) or sinks (Delta Lake, Iceberg) with minimal coupling.

---
## 📦 Repository Layout

| Path | Purpose |
|------|---------|
| `src/ingestion/csv_ingestion.py` | Ingest CSV defined in metadata to HDFS Parquet |
| `src/ingestion/rdbms_ingestion.py` | Ingest relational table(s) to Parquet |
| `src/ingestion/streaming_sub.py` | (Placeholder) potential streaming consumer logic |
| `src/kafka_api_pub/publisher_api.py` | FastAPI WebSocket endpoint that validates JSON and publishes to Kafka |
| `src/providers/hdfs_service.py` | HDFS + Parquet write utilities |
| `src/providers/rdbms_service.py` | DB connection + generic batch fetch abstraction |
| `src/utils/common_util_func.py` | Metadata loader + schema builder |
| `src/config/*.json` | Metadata configs (one per ocs group) |
| `requirements.txt` | Python dependencies |

---
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

### Parquet Writing
* Reorders row dicts to match schema field order.
* Writes one consolidated file: `<destination_path>/<dataset_name>.parquet`.
* Creates HDFS directory if absent.

---
## 🧪 Testing Strategy (Suggested)

Pending implementation. Recommended layers:
* Unit: schema mapping, metadata loading, identifier quoting per DB.
* Integration: mock DB cursor streaming; temporary local filesystem instead of HDFS (use `pyarrow.fs.LocalFileSystem()`).
* Contract: validate metadata JSON against JSONSchema.

---
## 🔧 Extending the Framework

| Extension | How |
|-----------|-----|
| New Source (e.g. REST API) | Add ingestion module producing `Iterable[List[Dict]]` batches compatible with `write_parquet_dataset` |
| Alternate Sink (e.g. S3) | Implement new provider using `pyarrow.fs.S3FileSystem` |
| Partitioned Output | Replace single-file writer with dataset partition writing (`pq.write_to_dataset`) using partition columns |
| Transformations | Populate `transformations` array objects (e.g. simple projection, casts) & apply pre‑write |
| Schema Evolution | Maintain versioned metadata; detect diffs & apply migrations |

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
1. Write to dated folders: `/data/warehouse/<dataset>/ingest_date=YYYY-MM-DD/part-*.parquet`.
2. Introduce a manifest table or Hive/Trino metastore integration.

---
## 🤝 Contributing
1. Fork & branch (`feat/<short-name>`).
2. Add or update metadata & code.
3. Add/update docs & (future) tests.
4. Submit PR with concise description & before/after notes.

Code style: prefer explicit imports, small pure functions, early validation.

---
## 📄 License
Specify license terms here (currently unspecified).

---
## 💬 Support / Help
Open an issue or reach maintainer(s). Include:
* Metadata file used
* Command executed
* Stack trace (if any)

---
## 🔮 Roadmap (Illustrative)
* [ ] Add JSONSchema validation for metadata
* [ ] Implement streaming consumer to persist Kafka → HDFS
* [ ] Partitioned Parquet & dataset statistics
* [ ] pluggable transformation engine
* [ ] Metrics & Prometheus endpoint
* [ ] Airflow / Dagster orchestration examples
* [ ] Docker Compose stack (HDFS, Kafka, API)

---
## 📚 Additional Documentation
See:
* `docs/architecture.md`
* `docs/metadata_schema.md`

---
## 🙌 Acknowledgements
Powered by PyArrow, FastAPI, kafka-python, and the broader open-source ecosystem.

