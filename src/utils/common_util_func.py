import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import os
import json
from typing import Dict, Iterable, List, Sequence
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
	'date': pa.date32(),
	'timestamp': pa.timestamp('s'),
}


def build_schema(features: Sequence[Dict]) -> pa.schema:
	fields = []
	for f in features:
		name = f['name']
		dtype = f.get('dtype', 'string').lower()
		pa_type = _PYARROW_TYPE_MAP.get(dtype, pa.string())
		fields.append(pa.field(name, pa_type))
	return pa.schema(fields)

#-------------------------------------------------------------------------------
# Read metadata config
#-------------------------------------------------------------------------------
def load_metadata(ocs_group: str) -> Dict:
	meta_path = f'src/config/{ocs_group}.json'
	if not os.path.exists(meta_path):
		raise FileNotFoundError(f"Metadata config not found: {meta_path}")
	with open(meta_path, 'r') as f:
		return json.load(f)