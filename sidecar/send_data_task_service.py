import requests 
import time
import logging
import json

def post_with_retry(url, payload, headers, max_retries=10, retry_delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(url, json=payload, headers=headers) 
            status_code_str = str(response.status_code)

            if status_code_str.startswith('2'):
                return response.json() 
            else:
                logging.info(f'Request failed with status code {response.status_code} on attempt {attempt}.')
                logging.info(f'Response: {response.text}')
                if attempt < max_retries:
                    logging.info(f'Retrying in {retry_delay} seconds...')
                    time.sleep(retry_delay)
                else:
                    logging.info('Max retry attempts reached. Giving up.')

        except requests.exceptions.RequestException as e:
            logging.error(f'An error occurred on attempt {attempt}: {e}')
            if attempt < max_retries:
                logging.info(f'Retrying in {retry_delay} seconds...')
                time.sleep(retry_delay)
            else:
                logging.error('Max retry attempts reached. Giving up.')
    return None

def prepare_post(url, param_names, param_types, param_optionals, kafka_info):
    payload = {
        "kafkaData": kafka_info,
        "params": {param: param_type for param, param_type in zip(param_names, param_types)},
        "optionalParams": param_optionals
    }
    logging.info(json.dumps(payload, indent=4))
    headers = {
        'Content-Type': 'application/json',
    }
    return post_with_retry(url, payload, headers, 10, 3)