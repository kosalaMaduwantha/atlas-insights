from __future__ import annotations

import os
import sys
import json
import logging
from typing import Dict, Iterable, List, Sequence

sys.path.append('/home/kosala/git-repos/atlas-insights/')  # ensure package root

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
try:  # optional imports for postgres
	import psycopg2
	from psycopg2.extras import RealDictCursor  # type: ignore
except Exception:  # pragma: no cover
	psycopg2 = None  # type: ignore
	RealDictCursor = None  # type: ignore
from src.config.config import HDFS_HOST, HDFS_PORT

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s - %(message)s')

# ----------------------------------------------------------------------------
# Schema & Type Utilities
# ----------------------------------------------------------------------------

_PYARROW_TYPE_MAP = {
	'int': pa.int64(),
	'integer': pa.int64(),
	'bigint': pa.int64(),
	'float': pa.float64(),
	'double': pa.float64(),
	'string': pa.string(),
	'text': pa.string(),
	'datetime': pa.timestamp('s'),  # store as seconds precision timestamp
	'timestamp': pa.timestamp('s'),
}


def _build_schema(features: Sequence[Dict]) -> pa.schema:
	fields = []
	for f in features:
		name = f['name']
		dtype = f.get('dtype', 'string').lower()
		pa_type = _PYARROW_TYPE_MAP.get(dtype, pa.string())
		fields.append(pa.field(name, pa_type))
	return pa.schema(fields)


# ----------------------------------------------------------------------------
# Database Extraction (Generic)
# ----------------------------------------------------------------------------

def _connect_db(source_cfg: Dict):
	"""Return a live DB connection and normalized db_type.

	Supports: postgresql/postgres, mysql/mariadb, mssql/sqlserver.
	Additional drivers imported lazily.
	"""
	db_type = (source_cfg.get('db_type') or 'postgresql').lower()
	sec_cfg = source_cfg.get('sec_config', {}) or {}
	host = source_cfg.get('host')
	port = source_cfg.get('port')
	database = source_cfg.get('database')

	if db_type in ('postgresql', 'postgres'):
		if psycopg2 is None:
			raise RuntimeError(
                'psycopg2 not installed. Add psycopg2-binary '
                'to requirements.txt')
		user = sec_cfg.get('user') 
		password = sec_cfg.get('password') 
		logger.info('Connecting to PostgreSQL host=%s db=%s', host, database)
		conn = psycopg2.connect(
			host=host,
			port=port or 5432,
			dbname=database,
			user=user,
			password=password,
			connect_timeout=10,
		)
		return conn, 'postgresql'

	if db_type in ('mysql', 'mariadb'):
        # lazy import only if needed
		try:
			import pymysql  # type: ignore
		except ImportError as e:  
			raise RuntimeError(
       'PyMySQL not installed. Add PyMySQL to requirements.txt') from e
		user = sec_cfg.get('user') 
		password = sec_cfg.get('password')
		logger.info('Connecting to MySQL host=%s db=%s', host, database)
		conn = pymysql.connect(
			host=host,
			port=int(port or 3306),
			db=database,
			user=user,
			password=password,
			charset='utf8mb4',
			cursorclass=pymysql.cursors.Cursor,
		)
		return conn, 'mysql'

	if db_type in ('mssql', 'sqlserver', 'sql_server'):
		try:
			import pyodbc  # type: ignore
		except ImportError as e:  # pragma: no cover
			raise RuntimeError('pyodbc not installed. Add pyodbc to requirements.txt') from e
		user = sec_cfg.get('user') 
		password = sec_cfg.get('password') 
		driver = sec_cfg.get('driver')
  
		logger.info('Connecting to SQL Server host=%s db=%s', host, database)
		conn_str = f'DRIVER={{{driver}}};SERVER={host},{port or 1433};DATABASE={database};UID={user};PWD={password}'
		conn = pyodbc.connect(conn_str, timeout=10)
		return conn, 'mssql'

	raise ValueError(f'Unsupported db_type: {db_type}')


def _quote_identifier(col: str, db_type: str) -> str:
	if db_type == 'postgresql':
		return f'"{col}"'
	if db_type == 'mysql':
		return f'`{col}`'
	if db_type == 'mssql':
		return f'[{col}]'
	return col


def _fetch_batches(
	conn,
	table_path: str,
	columns: Sequence[str],
	db_type: str,
	fetch_size: int = 10_000,
) -> Iterable[List[Dict]]:
	"""Generator yielding batches of rows as list[dict] generically."""
	quoted_cols = ', '.join([_quote_identifier(c, db_type) for c in columns])
	sql = f'SELECT {quoted_cols} FROM {table_path}'
	logger.info('Executing query: %s', sql)

	cursor_kwargs = {}
	use_dict_cursor = False
	if db_type == 'postgresql' and psycopg2 is not None and RealDictCursor is not None:
		cursor_kwargs['cursor_factory'] = RealDictCursor  # type: ignore
		use_dict_cursor = True

	with conn.cursor(**cursor_kwargs) as cur:  # type: ignore
		try:
			if hasattr(cur, 'itersize'):
				cur.itersize = fetch_size  # streaming optimization (postgres)
		except Exception:  # pragma: no cover
			pass
		cur.execute(sql)
		col_names = None
		while True:
			rows = cur.fetchmany(fetch_size)
			if not rows:
				break
			if use_dict_cursor:
				yield rows
			else:
				if col_names is None:
					col_names = [d[0] for d in cur.description]
				yield [dict(zip(col_names, r)) for r in rows]


# ----------------------------------------------------------------------------
# Parquet Writing (HDFS)
# ----------------------------------------------------------------------------

def _open_hdfs(host: str, port: int):
	return pafs.HadoopFileSystem(host=host, port=port)


def _ensure_hdfs_dir(hdfs: pafs.HadoopFileSystem, path: str):
	try:
		hdfs.create_dir(path)
	except FileExistsError:  # pragma: no cover
		pass
	except Exception:
		pass


def _write_parquet_dataset(
	batches: Iterable[List[Dict]],
	schema: pa.schema,
	dataset_name: str,
	destination_path: str,
	hdfs_host: str,
	hdfs_port: int,
):
	hdfs = _open_hdfs(hdfs_host, hdfs_port)
	_ensure_hdfs_dir(hdfs, destination_path)
	file_path = f"{destination_path.rstrip('/')}/{dataset_name}.parquet"
	logger.info("Writing Parquet to HDFS: %s", file_path)

	with hdfs.open_output_stream(file_path) as out_stream:
		writer = None
		try:
			for batch_rows in batches:
				if not batch_rows:
					continue
				table = _rows_to_table(batch_rows, schema)
				if writer is None:
					writer = pq.ParquetWriter(out_stream, table.schema)
				writer.write_table(table)
		finally:
			if writer is not None:
				writer.close()

	logger.info("Completed Parquet write: %s", file_path)


def _rows_to_table(rows: List[Dict], schema: pa.schema) -> pa.Table:
	"""Reorganize rows by column preserving order of schema fields"""
	columns = {}
	for field in schema:
		values = []
		for r in rows:
			values.append(r.get(field.name))
		array = pa.array(values, type=field.type, from_pandas=True)
		columns[field.name] = array
	return pa.table(columns, schema=schema)


# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------

def ingest_rdbms_to_parquet(
	metadata: Dict,
	ocs_group: str,
	hdfs_host: str = HDFS_HOST,
	hdfs_port: int = HDFS_PORT,
	fetch_size: int = 10_000,
) -> None:
	"""Ingest all datasets defined in metadata from RDBMS to HDFS Parquet."""
	source_cfg = metadata.get('source_config', {})
	conn, norm_db = _connect_db(source_cfg)
	try:
		with conn:
			for ds in metadata.get('dataset_config', []):
				source = ds.get('source', {})
				destination = ds.get('destination', {})
				dataset_name = source.get('name', ocs_group)
				table_path = source['path']
				features = source.get('features', [])
				column_names = [f['name'] for f in features]
				schema = _build_schema(features)
				dest_path = destination.get('path')
				if not dest_path:
					raise ValueError('Destination path not specified in metadata')

				logger.info(
					'Starting ingestion dataset=%s table=%s columns=%s -> %s', dataset_name, table_path, len(column_names), dest_path
				)

				batches = _fetch_batches(conn, table_path, column_names, db_type=norm_db, fetch_size=fetch_size)
				_write_parquet_dataset(
					batches=batches,
					schema=schema,
					dataset_name=dataset_name,
					destination_path=dest_path,
					hdfs_host=hdfs_host,
					hdfs_port=hdfs_port,
				)

				logger.info('Finished dataset=%s', dataset_name)
	finally:
		try:
			conn.close()
		except Exception:  # pragma: no cover
			pass


def load_metadata(ocs_group: str) -> Dict:
	meta_path = f'src/config/{ocs_group}.json'
	if not os.path.exists(meta_path):
		raise FileNotFoundError(f"Metadata config not found: {meta_path}")
	with open(meta_path, 'r') as f:
		return json.load(f)


def invoke_rdbms_ingestion(ocs_group: str = 'ecommerce_transactions_rdbms'):
	metadata = load_metadata(ocs_group)
	ingest_rdbms_to_parquet(metadata=metadata, ocs_group=ocs_group)


if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser(description='Ingest RDBMS dataset(s) defined by metadata JSON to Parquet on HDFS.')
	parser.add_argument('--ocs', '--ocs-group', dest='ocs_group', default='ecommerce_transactions_rdbms', help='OCS group / metadata JSON name (without .json)')
	parser.add_argument('--fetch-size', dest='fetch_size', type=int, default=10_000, help='Row fetch size per batch')
	args = parser.parse_args()

	metadata_cfg = load_metadata(args.ocs_group)
	ingest_rdbms_to_parquet(metadata_cfg, args.ocs_group, fetch_size=args.fetch_size)
	logger.info('Ingestion completed for %s', args.ocs_group)

