import asyncio
import json
import logging
from kafka import KafkaProducer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import HTMLResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    ocs_group: str = Query(None)
):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            try:
                json_data = json.loads(data)
                # validate the data
                metadata_config = {}
                meta_data_path = f'src/config/{ocs_group}.json'
                with open(meta_data_path, 'r') as f:
                    metadata_config = json.load(f)
                dataset_config = metadata_config.get('dataset_config', [{}])
                for config in dataset_config:
                    source_config = config.get('source', {})\
                                          .get('features', [])
                    validated_data = validate_data(
                        json_data, source_config)
                    logger.info(f"Validated data: {validated_data}")
                    # publish to Kafka
                    publish_to_kafka(validated_data, topic=ocs_group)

                response = {"status": "processed", "received": json_data}
            except json.JSONDecodeError:
                logger.debug("Received invalid JSON")
                response = {"status": "error", "message": "Invalid JSON"}
            await websocket.send_text(json.dumps(response))
    except WebSocketDisconnect:
        logger.info("Client disconnected")
        
def validate_data(data: dict, meta_data_keys: list) -> dict:
    output = {}
    try:
        for key in meta_data_keys:
            output[key['name']] = data[key['name']]
    except KeyError as e:
        logger.error(f"Missing key in data: {e}")
        raise e
    return output

def publish_to_kafka(data: dict, topic: str) -> bool:
    """Publish data to Kafka topic using kafka-python."""
    output = True
    try:
        producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info(f"Publishing to Kafka topic {topic}: {data}")
        producer.send(topic, value=data)
        producer.flush()
        producer.close()
        
    except Exception as e:
        logger.error(f"Error publishing to Kafka topic {topic}: {e}")
        producer.close()
        output = False
    return output