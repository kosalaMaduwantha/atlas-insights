import logging
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import datetime
import pandas as pd
from typing import Dict, Iterable, List
from pyarrow import orc

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s - %(message)s')

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

def write_parquet_dataset(
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
 
def write_orc_dataset(
	batches: Iterable[List[Dict]],
	schema: pa.schema,
	dataset_name: str,
	destination_path: str,
	hdfs_host: str,
	hdfs_port: int,
):
	hdfs = _open_hdfs(hdfs_host, hdfs_port)
	_ensure_hdfs_dir(hdfs, destination_path)
	file_path = f"{destination_path.rstrip('/')}/{dataset_name}.orc"
	logger.info("Writing ORC to HDFS: %s", file_path)

	with hdfs.open_output_stream(file_path) as out_stream:
		writer = None
		try:
			for batch_rows in batches:
				if not batch_rows:
					continue
				table = _rows_to_table(batch_rows, schema)
				if writer is None:
					writer = orc.ORCWriter(out_stream)
				writer.write(table)
		finally:
			if writer is not None:
				writer.close()

	logger.info("Completed ORC write: %s", file_path)
 
def _rows_to_table(rows: List[Dict], schema: pa.schema) -> pa.Table:
	"""Reorganize rows by column preserving order of schema fields"""
	try:
		columns = {}
		for field in schema:
			values = []
			for r in rows:
				values.append(r.get(field.name))
			
			# Handle timestamp conversion from string
			if pa.types.is_timestamp(field.type):
				# Convert string datetime to pandas datetime, then to pyarrow
				values = pd.to_datetime(values, errors='coerce')
				array = pa.array(values, type=field.type)
			else:
				array = pa.array(values, type=field.type, from_pandas=True)
			
			columns[field.name] = array
	except Exception as e:
		logger.error(f"Error converting rows to table: {e}")
		raise e
	return pa.table(columns, schema=schema)