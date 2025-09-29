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
from src.utils.common_util_func import build_schema, load_metadata
from src.providers.hdfs_service import write_parquet_dataset
from src.providers.rdbms_service import connect_db, fetch_batches
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
	conn, norm_db = connect_db(source_cfg)
	try:
		with conn:
			for ds in metadata.get('dataset_config', []):
				source = ds.get('source', {})
				destination = ds.get('destination', {})
				dataset_name = source.get('name', ocs_group)
				table_path = source['path']
				features = source.get('features', [])
				column_names = [f['name'] for f in features]
				schema = build_schema(features)
				dest_path = destination.get('path')
				if not dest_path:
					raise ValueError('Destination path not specified in metadata')

				logger.info(
					'Starting ingestion dataset=%s table=%s columns=%s -> %s', dataset_name, table_path, len(column_names), dest_path
				)

				batches = fetch_batches(conn, table_path, column_names, db_type=norm_db, fetch_size=fetch_size)
				write_parquet_dataset(
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

