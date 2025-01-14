# Configuration
import os
from fastapi import requests
from kafka import KafkaConsumer, KafkaProducer
import logging
import json

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',  
    handlers=[
        logging.StreamHandler()
    ]
)

from sidecar.send_data_task_service import prepare_post

# Creo que quizás hace falta ponerle una config al logger
logging.info("Empezando el sidecar!")
# Usar ENV variables, esos son los default que habría que setearlos apropiadamente
KAFKA_AGENT_TOPIC = os.getenv("KAFKA_AGENT_TOPIC", "example_topic")
KAFKA_RESPONSE_TOPIC = os.getenv("KAFKA_RESPONSE_TOPIC")
# Puede ser un array de brokers
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
AGENT_URL = os.getenv("AGENT_URL", "http://localhost:8000/echo")
USERNAME = os.getenv("KAFKA_USERNAME", "user")
PASSWORD = os.getenv("KAFKA_PASSWORD", "pass")

TASK_SERVICE_URL = os.getenv("TASK_SERVICE_URL", "")
TASK_NAME = os.getenv("TASK_NAME", "echo")
PARAM_NAMES = os.getenv("PARAM_NAMES", [""])
PARAM_TYPES = os.getenv("PARAM_TYPES", [""])
PARAM_OPTIONALS = os.getenv("PARAM_OPTIONALS", [""])


logging.info(f"Using TOPIC: {KAFKA_AGENT_TOPIC}, Kafka Bootstrap Server: {KAFKA_BROKERS}, Agent URL: {AGENT_URL}")
logging.info(f"Task Service URL: {TASK_SERVICE_URL}, Task name: {TASK_NAME}, Param names: {PARAM_NAMES}, Param types: {PARAM_TYPES}, Param optionals: {PARAM_OPTIONALS}")

consumer = KafkaConsumer(
    KAFKA_AGENT_TOPIC,
    bootstrap_servers=KAFKA_BROKERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='sidecar-group',
    # TODO: Revisar que esto tenga sentido
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8') 
)

kafka_info = {
    "broker": KAFKA_BROKERS,
    "topic": KAFKA_AGENT_TOPIC,
    "username": USERNAME,
    "password": PASSWORD
}

prepare_post(TASK_SERVICE_URL, TASK_NAME, PARAM_NAMES, PARAM_TYPES, PARAM_OPTIONALS, kafka_info)


def send_to_echo_service(message):
    try:
        response = requests.post(AGENT_URL, json={"message": message})
        response.raise_for_status()
        logging.log(f"Successfully sent message to echo service: {response.json()}")
        producer.send(
            KAFKA_RESPONSE_TOPIC,  # Target Kafka topic
            {"message": response.json()}  # Send the response as a JSON message
        )
        logging.info(f"Successfully sent response to Kafka topic: {KAFKA_RESPONSE_TOPIC}")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send message to echo service: {e}")
        producer.send(
            KAFKA_RESPONSE_TOPIC,  # Error topic
            {"error": str(e), "original_message": message}  # Send error details
        )
        logging.info(f"Error sent to Kafka topic: {KAFKA_RESPONSE_TOPIC}")

logging.info(f"Listening to Kafka topic: {KAFKA_AGENT_TOPIC}")

# The for message in consumer loop will block, waiting for new messages to arrive in the specified KAFKA_TOPIC.
for message in consumer:
    message_value = message.value.decode('utf-8')
    logging.log(f"Received message from Kafka: {message_value}")
    send_to_echo_service(message_value)