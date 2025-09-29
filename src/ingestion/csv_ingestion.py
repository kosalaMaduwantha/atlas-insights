import sys
sys.path.append('/home/kosala/git-repos/atlas-insights/')
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import json
from src.config.config import HDFS_HOST, HDFS_PORT

def csv_to_parquet_hdfs(
        dataset_config: dict,
        hdfs_host=HDFS_HOST, 
        hdfs_port=HDFS_PORT
    ):
    """Convert a local CSV file to Parquet format and upload it to HDFS.
    Args:
        dataset_config (dict): Configuration dictionary containing dataset details.
        hdfs_path (str): HDFS directory path where the Parquet file will be stored.
        hdfs_host (str): Hostname of the HDFS namenode.
        hdfs_port (int): Port number of the HDFS namenode.
    """
    for dataset in dataset_config:
        destination = dataset.get('destination', {})
        hdfs_path = destination.get('path', '/user/kosala/input')
        source_dataset_config = dataset.get('source', {})
        table = pv.read_csv(source_dataset_config.get('path'))

        # filter columns defined in columns_to_keep in metadata
        filtered_table = table.select([col.get('name') for col in \
            source_dataset_config.get('features', [])])
        
        # convert to parquet in memory
        sink = pa.BufferOutputStream()
        pq.write_table(filtered_table, sink)
        parquet_buffer = sink.getvalue()

        hdfs = pafs.HadoopFileSystem(
            host=hdfs_host, port=hdfs_port)
        with hdfs.open_output_stream(hdfs_path + '/' + \
            source_dataset_config.get('name') + '.parquet') as out_stream:
            out_stream.write(parquet_buffer.to_pybytes())

def invok_e_csv_to_parquet(ocs_name='ecommerce_transactions_fs'):
    """Invoke the CSV to Parquet conversion with example parameters."""
    
    metadata_config = {}
    with open(f'src/config/{ocs_name}.json', 'r') as f:
        metadata_config = json.load(f)

    # Extract relevant info from metadata
    dataset_config = metadata_config.get('dataset_config', [{}])
    
    hdfs_host = HDFS_HOST
    hdfs_port = HDFS_PORT

    csv_to_parquet_hdfs(
        dataset_config=dataset_config,
        hdfs_host=hdfs_host,
        hdfs_port=hdfs_port
    )

# Example usage:
if __name__ == "__main__":
    invok_e_csv_to_parquet()
        