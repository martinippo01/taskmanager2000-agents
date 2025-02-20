# Configuration
import os
import threading
import requests
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

from healthcheck import start_fastapi
from send_data_task_service import prepare_post

# Creo que quizás hace falta ponerle una config al logger
logging.info("Empezando el sidecar!")
# Usar ENV variables, esos son los default que habría que setearlos apropiadamente
KAFKA_AGENT_TOPIC = os.getenv("KAFKA_TOPIC_STA", "example_topic")
KAFKA_RESPONSE_TOPIC = os.getenv("KAFKA_TOPIC_SER")
KAFKA_BROKERS_FROM_SCHEDULER = os.getenv("KAFKA_BROKERS_STA", "localhost:9092")
KAFKA_BROKERS_TO_ORQUESTADOR = os.getenv("KAFKA_BROKERS_SER", "localhost:9092")
KAFKA_GROUP_FROM_SCHEDULER = os.getenv("KAFKA_GROUP_ID_STA", "echo_group")
KAFKA_CLIENT_ID_FROM_SCHEDULER = os.getenv("KAFKA_CLIENT_ID_STA", "client_id_sche")
KAFKA_CLIENT_ID_TO_ORQUESTADOR = os.getenv("KAFKA_CLIENT_ID_SER", "client_id_sche")
AGENT_URL = os.getenv("AGENT_URL", "http://localhost:8000/echo")
USERNAME = os.getenv("KAFKA_USERNAME_STA", "user")
PASSWORD = os.getenv("KAFKA_PASSWORD_STA", "pass")

TASK_SERVICE_URL = os.getenv("TASK_SERVICE_URL", "")
TASK_NAME = os.getenv("TASK_NAME", "echo")
PARAM_NAMES = os.getenv("PARAM_NAMES", "").split(",") if os.getenv("PARAM_NAMES") else []
PARAM_TYPES = os.getenv("PARAM_TYPES", "").split(",") if os.getenv("PARAM_TYPES") else []
PARAM_OPTIONALS = os.getenv("PARAM_OPTIONALS", "").split(",") if os.getenv("PARAM_OPTIONALS") else []

NFS_PATH = os.getenv("NFS_PATH", "/answers")


logging.info(f"NFS: {NFS_PATH}")
logging.info(f"Using TOPIC: {KAFKA_AGENT_TOPIC}, Kafka To Get Steps From Scheduler: {KAFKA_BROKERS_FROM_SCHEDULER}, Agent URL: {AGENT_URL}")
logging.info(f"For Kafka Producer (to Orquestador): {KAFKA_BROKERS_TO_ORQUESTADOR}, Using Topic: {KAFKA_RESPONSE_TOPIC}")
logging.info(f"Task Service URL: {TASK_SERVICE_URL}, Task name: {TASK_NAME}, Param names: {PARAM_NAMES}, Param types: {PARAM_TYPES}, Param optionals: {PARAM_OPTIONALS}")


# FastAPI runs in the background
portForHealthcheck = os.getenv("HEALTHCHECK_PORT", 8500)
logging.info(f"En otro thread va a estar funcionando el FastAPI con el healthcheck en el puerto {portForHealthcheck}")
threading.Thread(target=start_fastapi, daemon=True).start()  


consumer = KafkaConsumer(
    KAFKA_AGENT_TOPIC,
    bootstrap_servers=KAFKA_BROKERS_FROM_SCHEDULER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=KAFKA_GROUP_FROM_SCHEDULER,
    client_id=KAFKA_CLIENT_ID_FROM_SCHEDULER,
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Para deserializar
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS_TO_ORQUESTADOR,
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    client_id=KAFKA_CLIENT_ID_TO_ORQUESTADOR,
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


kafka_info = {
    "brokers": KAFKA_BROKERS_FROM_SCHEDULER,
    "topic": KAFKA_AGENT_TOPIC,
    "username": USERNAME,
    "password": PASSWORD
}

# Esto es para el task service, le estoy enviando la data del tópico de Kafka y esas cosas
if prepare_post(TASK_SERVICE_URL, PARAM_NAMES, PARAM_TYPES, PARAM_OPTIONALS, kafka_info) == None:
    logging.error("El sidecar no pudo darle la información necesaria al task service :(((")
    raise Exception
# A partir de acá termina el setupeo

def save_to_nfs(message, path_to_nfs: str):
    try:
        # Esto crea el directorio si no existe
        os.makedirs(os.path.dirname(path_to_nfs), exist_ok=True)

        # Check if the file already exists
        if not os.path.exists(path_to_nfs):
            with open(path_to_nfs, "w") as file:
                file.write(f"{message}")
            logging.info(f"Message successfully saved to NFS: {path_to_nfs}")
            return True
        else:
            logging.info(f"File already exists, not writing to NFS: {path_to_nfs}")
            return False
    except Exception as e:
        logging.error(f"Failed to write to NFS: {e}")
        return False

# En where_to_look se va a cargar el path al nfs
def send_response_to_kafka(execId: str, where_to_look: str, name: str):
    try:
        producer.send(
            KAFKA_RESPONSE_TOPIC,  
            {"executionId": execId, "answer": where_to_look, "name": name}  
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
        saved = save_to_nfs(response_json.get("outcome", "Ups!"), path_save_in_nfs)
        if saved:
            send_response_to_kafka(workflow_execution_id, path_save_in_nfs, name)
    except Exception as e:
        logging.error(f"Failed to process message: {e}")