import sys
sys.path.append('/home/kosala/git-repos/atlas-insights/')

import argparse
import json
import logging
from typing import Dict, List
from kafka import KafkaConsumer
from datetime import datetime

from src.config.config import HDFS_HOST, HDFS_PORT
from src.utils.common_util_func import build_schema, load_metadata
from src.providers.hdfs_service import write_parquet_dataset

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s - %(message)s')


def consume_and_ingest_to_hdfs(
	metadata: Dict,
	ocs_group: str,
	topic: str,
	bootstrap_servers: str = 'localhost:9092',
	group_id: str = 'atlas-insights-consumer',
	batch_size: int = 1000,
	hdfs_host: str = HDFS_HOST,
	hdfs_port: int = HDFS_PORT,
):
	"""
	Consume messages from Kafka topic and write batches to HDFS as Parquet.
	
	Args:
		metadata: Configuration dictionary with dataset_config
		ocs_group: OCS group name
		topic: Kafka topic to consume from
		bootstrap_servers: Kafka bootstrap servers
		group_id: Kafka consumer group id
		batch_size: Number of messages to accumulate before writing to HDFS
		hdfs_host: HDFS host
		hdfs_port: HDFS port
	"""
	# Get dataset configuration from metadata
	dataset_configs = metadata.get('dataset_config', [])
	if not dataset_configs:
		raise ValueError('No dataset configuration found in metadata')
	
	ds = dataset_configs[0]  # Assuming single dataset for streaming
	source = ds.get('source', {})
	destination = ds.get('destination', {})
	dataset_name = source.get('name', ocs_group)
	features = source.get('features', [])
	dest_path = source.get('path')  # Get path from source config
	
	if not dest_path:
		raise ValueError('Destination path not specified in metadata')
	
	# Build schema from features
	schema = build_schema(features)
	column_names = [f['name'] for f in features]
	
	logger.info(
		'Starting Kafka consumer: topic=%s group_id=%s batch_size=%d -> %s',
		topic, group_id, batch_size, dest_path
	)
	# Initialize Kafka consumer
	consumer = KafkaConsumer(
		topic,
		bootstrap_servers=bootstrap_servers,
		group_id=group_id,
		auto_offset_reset='earliest',
		enable_auto_commit=True,
		value_deserializer=lambda m: json.loads(m.decode('utf-8'))
	)
	
	batch_buffer: List[Dict] = []
	message_count = 0
	
	try:
		for message in consumer:
			try:
				# Parse message value (already deserialized as JSON)
				record = message.value
				# Validate and filter columns based on schema
				filtered_record = {}
				for col in column_names:
					if col in record:
						filtered_record[col] = record[col]
					else:
						filtered_record[col] = None  # Handle missing columns
				
				batch_buffer.append(filtered_record)
				message_count += 1
				
				# Write batch to HDFS when buffer reaches batch_size
				if len(batch_buffer) >= batch_size:
					logger.info('Writing batch of %d records to HDFS', len(batch_buffer))
					write_parquet_dataset(
						batches=[batch_buffer],
						schema=schema,
						dataset_name=f"{dataset_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
						destination_path=dest_path,
						hdfs_host=hdfs_host,
						hdfs_port=hdfs_port,
					)
					batch_buffer.clear()
					logger.info('Total messages processed: %d', message_count)
					
			except json.JSONDecodeError as e:
				logger.error('Failed to parse message: %s', e)
				continue
			except Exception as e:
				logger.error('Error processing message: %s', e)
				continue
				
	except KeyboardInterrupt:
		logger.info('Consumer stopped by user')
	finally:
		# Write any remaining records in buffer
		if batch_buffer:
			logger.info('Writing final batch of %d records to HDFS', len(batch_buffer))
			write_parquet_dataset(
				batches=[batch_buffer],
				schema=schema,
				dataset_name=f"{dataset_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
				destination_path=dest_path,
				hdfs_host=hdfs_host,
				hdfs_port=hdfs_port,
			)
		consumer.close()
		logger.info('Consumer closed. Total messages processed: %d', message_count)


def invoke_streaming_ingestion(
	ocs_group: str = 'ecommerce_transactions_streaming',
	topic: str = 'ecommerce_transactions',
	bootstrap_servers: str = 'localhost:9092',
	batch_size: int = 1000,
):
	"""
	Invoke streaming ingestion pipeline.
	
	Args:
		ocs_group: OCS group / metadata JSON name (without .json)
		topic: Kafka topic to consume from
		bootstrap_servers: Kafka bootstrap servers
		batch_size: Number of messages to accumulate before writing to HDFS
	"""
	metadata = load_metadata(ocs_group)
	consume_and_ingest_to_hdfs(
		metadata=metadata,
		ocs_group=ocs_group,
		topic=topic,
		bootstrap_servers=bootstrap_servers,
		batch_size=batch_size,
	)


if __name__ == "__main__":
	parser = argparse.ArgumentParser(
		description='Consume from Kafka topic and ingest to HDFS Parquet based on metadata config.'
	)
	parser.add_argument(
		'--ocs', '--ocs-group',
		dest='ocs_group',
		default='ecommerce_transactions_streaming',
		help='OCS group / metadata JSON name (without .json)'
	)
	parser.add_argument(
		'--topic',
		required=True,
		help='Kafka topic to consume from'
	)
	parser.add_argument(
		'--bootstrap-servers',
		default='localhost:9092',
		help='Kafka bootstrap servers (comma-separated)'
	)
	parser.add_argument(
		'--group-id',
		default='atlas-insights-consumer',
		help='Kafka consumer group id'
	)
	parser.add_argument(
		'--batch-size',
		type=int,
		default=1000,
		help='Number of messages to accumulate before writing to HDFS'
	)
	
	args = parser.parse_args()
	
	metadata_cfg = load_metadata(args.ocs_group)
	consume_and_ingest_to_hdfs(
		metadata=metadata_cfg,
		ocs_group=args.ocs_group,
		topic=args.topic,
		bootstrap_servers=args.bootstrap_servers,
		group_id=args.group_id,
		batch_size=args.batch_size,
	)

# Example usage:
# python src/ingestion/streaming_sub.py --topic ecommerce_transactions --ocs ecommerce_transactions_streaming --batch-size 1000

# python src/ingestion/streaming_sub.py \
#   --topic ecommerce_transactions \
#   --ocs ecommerce_transactions_streaming \
#   --batch-size 1000