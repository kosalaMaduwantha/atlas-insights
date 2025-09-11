import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import json
from src.config.config import HDFS_HOST, HDFS_PORT

def csv_to_parquet_hdfs(
        local_csv_path, 
        columns_to_keep, 
        parquet_file_name, 
        hdfs_path, 
        hdfs_host='localhost', 
        hdfs_port=8020
    ):
    """Convert a local CSV file to Parquet format and upload it to HDFS.
    Args:
        local_csv_path (str): Path to the local CSV file.
        columns_to_keep (list): List of column names to retain in the Parquet file.
        parquet_file_name (str): Name of the output Parquet file.
        hdfs_path (str): HDFS directory path where the Parquet file will be stored.
        hdfs_host (str): Hostname of the HDFS namenode.
        hdfs_port (int): Port number of the HDFS namenode.
    """
    table = pv.read_csv(local_csv_path)
    
    # filter columns defined in columns_to_keep in metadata
    filtered_table = table.select(columns_to_keep)
    
    # convert to parquet in memory
    sink = pa.BufferOutputStream()
    pq.write_table(filtered_table, sink)
    parquet_buffer = sink.getvalue()

    hdfs = pafs.HadoopFileSystem(host=hdfs_host, port=hdfs_port)
    with hdfs.open_output_stream(hdfs_path + '/' + parquet_file_name) as out_stream:
        out_stream.write(parquet_buffer.to_pybytes())

def invok_e_csv_to_parquet(ocs_name='ecommerce_transactions'):
    """Invoke the CSV to Parquet conversion with example parameters."""
    
    metadata_config = {}
    with open(f'src/config/{ocs_name}_meta_config.json', 'r') as f:
        metadata_config = json.load(f)

    # Extract relevant info from metadata
    dataset = metadata_config.get('dataset_config', [{}])[0]
    source = dataset.get('source', {})
    destination = dataset.get('destination', {})

    local_csv_path = dataset.get('source', {}).get('path')
    columns_to_keep = [feature['name'] for feature in source.get('features', [])]
    # Parquet file name: use destination name or source name + .parquet
    parquet_file_name = f"{destination.get('name', source.get('name', 'output'))}.parquet"
    # HDFS path: use destination path
    hdfs_path = destination.get('path', '/user/kosala/input')
    hdfs_host = HDFS_HOST
    hdfs_port = HDFS_PORT

    csv_to_parquet_hdfs(
        local_csv_path=local_csv_path,
        columns_to_keep=columns_to_keep,
        parquet_file_name=parquet_file_name,
        hdfs_path=hdfs_path,
        hdfs_host=hdfs_host,
        hdfs_port=hdfs_port
    )

# Example usage:
if __name__ == "__main__":
    invok_e_csv_to_parquet()
        