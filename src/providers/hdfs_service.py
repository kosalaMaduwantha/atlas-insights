import logging
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import datetime
from typing import Dict, Iterable, List

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