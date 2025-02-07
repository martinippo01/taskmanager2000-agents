# Configuration
import os
from fastapi import requests # quizas esto deba ser import requests
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

NFS_PATH = os.getenv("NFS_PATH", ".")


logging.info("NFS: {NFS_PATH}")
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
    sasl_plain_password=PASSWORD,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Para deserializar
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

# Esto es para el task service, le estoy enviando la data del tópico de Kafka y esas cosas
if prepare_post(TASK_SERVICE_URL, TASK_NAME, PARAM_NAMES, PARAM_TYPES, PARAM_OPTIONALS, kafka_info) == None:
    logging.error("El sidecar no pudo darle la información necesaria al task service :(((")
    raise Exception
# A partir de acá termina el setupeo

def save_to_nfs(message, path_to_nfs: str):
    try:
        with open(path_to_nfs, "a") as file:
            file.write(f"{message}\n")
        logging.info(f"Message successfully saved to NFS: {path_to_nfs}")
    except Exception as e:
        logging.error(f"Failed to write to NFS: {e}")

# En where_to_look se va a cargar el path al nfs
def send_response_to_kafka(execId: str, where_to_look: str):
    try:
        producer.send(
            KAFKA_RESPONSE_TOPIC,  
            {"executionId": execId, "answer": where_to_look}  
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
    # Los mensajes que llegues van a tener la siguiente pinta:
    # workflowExecutionId: string; name: string; inputArgs: InputArguments;
    try:
        logging.info(f"Received message from Kafka: {message}")
        data = message.value  # This is already a dictionary due to value_deserializer
        workflow_execution_id = data.get("workflowExecutionId")
        name = data.get("name")
        inputArgs = data.get("inputArgs")
        response = requests.post(AGENT_URL, json={"inputs": inputArgs})
        response.raise_for_status()
        logging.info(f"Successfully sent message to agent: {response.json()}")
        # response debe tener un campo llamado "outcome"
        path_save_in_nfs = f"{NFS_PATH}/{workflow_execution_id}/{name}"
        response_json = response.json()
        save_to_nfs(response_json.get("outcome", ""), path_save_in_nfs)
        send_response_to_kafka(workflow_execution_id, path_save_in_nfs)
    except Exception as e:
        logging.error("Failed to process message: {e}")