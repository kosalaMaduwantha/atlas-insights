# Metadata Schema Specification

This document formalizes the structure of the ingestion metadata JSON files located in `src/config/`.

## Top-Level Structure

```jsonc
{
  "ocs_group_name": "string",
  "source_type": "fs | rdbms | streaming | <future> api | object_store",
  "source_config": { /* depends on source_type */ },
  "dataset_config": [ DatasetConfig, ... ]
}
```

## Field Definitions

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ocs_group_name` | string | yes | Logical ingestion group identifier (also reused as Kafka topic optionally). |
| `source_type` | enum | yes | Nature of upstream system (`fs`, `rdbms`, `streaming`). |
| `source_config` | object | yes | Connection or format info; shape differs per source. |
| `dataset_config` | array | yes | One or more dataset extraction definitions. |

## `source_config` Variants

### File System (`fs`)
```jsonc
{
  "file_format": "csv|json|parquet",
  "host": "string?",    // optional reference context
  "port": null,           // currently unused
  "sec_config": {}        // reserved for auth (future)
}
```

### RDBMS (`rdbms`)
```jsonc
{
  "db_type": "postgresql|mysql|mariadb|mssql|sqlserver",
  "host": "string",
  "port": 5432,
  "database": "string",
  "sec_config": {
    "user": "string",
    "password": "string"
  }
}
```

### Streaming (`streaming`)
```jsonc
{
  "sec_config": { }
}
```

## `DatasetConfig` Object

```jsonc
{
  "source": SourceSpec,
  "transformations": [ /* future operations */ ],
  "destination": DestinationSpec
}
```

### `SourceSpec`
| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `name` | string | yes | Logical name; used for output file naming if destination name omitted. |
| `primary_key` | string | no | Informational; not currently enforced. |
| `path` | string | yes | Filesystem path (fs), table name (rdbms), stream/topic id (streaming). |
| `features` | array[Feature] | yes | Schema definition (order preserved). |

### `DestinationSpec`
| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `name` | string | no | Defaults to `source.name` when absent. |
| `primary_key` | string | no | Placeholder for downstream modeling. |
| `path` | string | yes (batch) | HDFS/base output location. Not required for streaming prototype. |
| `features` | array[Feature] | no | Optional explicit target schema; if omitted, source schema used. |

### `Feature` Object
```jsonc
{ "name": "ColumnName", "dtype": "int|string|float|double|datetime|timestamp|bigint|text|date" }
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Column identifier (case-sensitive as stored). |
| `dtype` | string | yes | Logical data type mapped to PyArrow. |

## Supported Logical Types

| Logical | Maps To | Notes |
|---------|---------|-------|
| `int`, `integer`, `bigint` | `pa.int64()` | Uniform 64-bit representation. |
| `float`, `double` | `pa.float64()` | Currently both mapped identically. |
| `string`, `text` | `pa.string()` | UTF-8. |
| `datetime`, `timestamp` | `pa.timestamp('s')` | Seconds precision. |
| `date` | (planned) `pa.date32()` | Present in metadata; add to code for full support. |

## Validation Guidelines (Recommended)

1. Ensure `dataset_config` is non-empty.
2. Each dataset must have at least one feature.
3. Destination path required for non-streaming ingestion.
4. Disallow duplicate feature names (case-insensitive) within a dataset.
5. Enforce dtype membership in supported logical set.
6. Optional: JSON Schema validation stage prior to run.

## Example (RDBMS)

```jsonc
{
  "ocs_group_name": "ecommerce_transactions_rdbms",
  "source_type": "rdbms",
  "source_config": {
    "db_type": "mysql",
    "host": "localhost",
    "port": 3306,
    "database": "ecommerce_db",
    "sec_config": { "user": "root", "password": "example" }
  },
  "dataset_config": [
    {
      "source": {
        "name": "ecommerce_transactions_db",
        "primary_key": "transaction_id",
        "path": "ecommerce_transactions",
        "features": [
          {"name": "InvoiceNo", "dtype": "int"},
          {"name": "StockCode", "dtype": "string"}
        ]
      },
      "transformations": [],
      "destination": {
        "name": "ecommerce_transactions_warehouse",
        "path": "/data/warehouse/ecommerce_transactions_db"
      }
    }
  ]
}
```

## Evolution Strategy

| Change Type | Handling Recommendation |
|-------------|-------------------------|
| Add Column | Append feature; downstream readers must tolerate new fields. |
| Remove Column | Soft-deprecate first; maintain nulls before physical drop. |
| Type Change | Write new version folder; migrate consumers gradually. |
| Rename Column | Treat as remove + add; optionally keep alias map. |

## JSON Schema (Draft)

Below is a draft JSON Schema excerpt (trimmed for readability):

```jsonc
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.com/ingestion-metadata.schema.json",
  "type": "object",
  "required": ["ocs_group_name", "source_type", "source_config", "dataset_config"],
  "properties": {
    "ocs_group_name": {"type": "string"},
    "source_type": {"enum": ["fs", "rdbms", "streaming"]},
    "source_config": {"type": "object"},
    "dataset_config": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["source", "destination"],
        "properties": {
          "source": {
            "type": "object",
            "required": ["name", "path", "features"],
            "properties": {
              "name": {"type": "string"},
              "path": {"type": "string"},
              "features": {
                "type": "array",
                "minItems": 1,
                "items": {
                  "type": "object",
                  "required": ["name", "dtype"],
                  "properties": {
                    "name": {"type": "string"},
                    "dtype": {"type": "string"}
                  }
                }
              }
            }
          },
          "destination": {
            "type": "object",
            "required": ["path"],
            "properties": {
              "path": {"type": "string"}
            }
          }
        }
      }
    }
  }
}
```

## Future Extensions

| Addition | Rationale |
|----------|-----------|
| `partition_columns` | Enable partitioned parquet datasets. |
| `write_mode` (overwrite, append, merge) | Control ingestion semantics. |
| `transformations` objects | Declarative transformation chain (select, cast, derive). |
| `quality_rules` | Enforce nullability, domain constraints. |
| `encryption` | Sensitive column handling at rest. |

---

This specification should evolve under version control; consider tagging with a semantic version (e.g., `schema_version`).
