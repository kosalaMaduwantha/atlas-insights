import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow.fs as pafs

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

# Example usage:
if __name__ == "__main__":
    csv_to_parquet_hdfs(
        local_csv_path='/home/kosala/git-repos/atlas-insights/data/e_commerce_transactions_dataset/ecommerce_transactions.csv',
        columns_to_keep=['Transaction_ID','User_Name','Age','Country','Product_Category','Purchase_Amount','Payment_Method','Transaction_Date'],
        parquet_file_name='ecommerce_output.parquet',
        hdfs_path='/user/kosala/input',
        hdfs_host='localhost',
        hdfs_port=9000
    )