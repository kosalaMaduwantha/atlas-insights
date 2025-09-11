import os

HDFS_HOST = os.getenv('HDFS_HOST', 'localhost')
HDFS_PORT = int(os.getenv('HDFS_PORT', 9000))