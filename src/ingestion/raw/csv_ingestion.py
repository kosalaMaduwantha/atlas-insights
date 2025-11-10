
import sys
sys.path.append('/home/kosala/git-repos/atlas-insights/')
import pyarrow as pa
import pyarrow.csv as pv
import logging
from src.config.config import HDFS_HOST, HDFS_PORT
from src.utils.common_util_func import build_schema, load_metadata
from src.providers.hdfs_service import write_parquet_dataset, write_orc_dataset
import os
import json


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s - %(message)s')

def ingest_csv_to_parquet(
    metadata: dict,
    ocs_group: str,
    hdfs_host: str = HDFS_HOST,
    hdfs_port: int = HDFS_PORT,
):
    """Ingest all datasets defined in metadata from CSV to HDFS Parquet using utility functions."""
    for ds in metadata.get('dataset_config', []):
        source = ds.get('source', {})
        destination = ds.get('destination', {})
        dataset_name = source.get('name', ocs_group)
        csv_path = source.get('path')
        features = source.get('features', [])
        column_names = [f['name'] for f in features]
        
        schema = build_schema(features)
        dest_path = destination.get('path')
        if not dest_path:
            raise ValueError('Destination path not specified in metadata')

        logger.info(
            'Starting CSV ingestion dataset=%s csv=%s columns=%s -> %s', dataset_name, csv_path, len(column_names), dest_path
        )

        table = pv.read_csv(csv_path)
        filtered_table = table.select(column_names)
        # Convert filtered_table to batches of dicts for write_parquet_dataset
        batch_dicts = [dict(zip(filtered_table.schema.names, row)) for row in zip(*[filtered_table.column(i).to_pylist() for i in range(filtered_table.num_columns)])]
        # Wrap in a list to match Iterable[List[Dict]]
        batches = [batch_dicts]

        write_orc_dataset(
            batches=batches,
            schema=schema,
            dataset_name=dataset_name,
            destination_path=dest_path,
            hdfs_host=hdfs_host,
            hdfs_port=hdfs_port,
        )

        logger.info('Finished dataset=%s', dataset_name)


def invoke_csv_ingestion(ocs_group: str = 'ecommerce_transactions_fs'):
    metadata = load_metadata(ocs_group)
    ingest_csv_to_parquet(metadata=metadata, ocs_group=ocs_group)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Ingest CSV dataset(s) defined by metadata JSON to Parquet on HDFS.')
    parser.add_argument('--ocs', '--ocs-group', dest='ocs_group', default='ecommerce_transactions_fs', help='OCS group / metadata JSON name (without .json)')
    args = parser.parse_args()

    metadata_cfg = load_metadata(args.ocs_group)
    ingest_csv_to_parquet(metadata_cfg, args.ocs_group)
    logger.info('CSV ingestion completed for %s', args.ocs_group)
        