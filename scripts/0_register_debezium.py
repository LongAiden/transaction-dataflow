import requests
import json
import os
import sys
import time
import datetime as dt

KAFKA_RETRIES = 30
KAFKA_SLEEP = 5

def check_connector_exists():
    try:
        response = requests.get('http://localhost:8083/connectors/postgres-connector')
        return response.status_code == 200
    except requests.exceptions.RequestException as e:
        return False

def check_kafka_connect_health():
    try:
        response = requests.get('http://localhost:8083')
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def register_connector():
    # Check if connector already exists
    if check_connector_exists():
        print("Connector already registered, checking status...")
        response = requests.get('http://localhost:8083/connectors/postgres-connector/status')
        print(f"Connector status: {response.json()}")
        sys.exit(0)

    # Wait for Kafka Connect to be ready
    retries = KAFKA_RETRIES
    while retries > 0:
        if check_kafka_connect_health():
            break
        print("Waiting for Kafka Connect to be ready...")
        time.sleep(KAFKA_SLEEP)
        retries -= 1
    
    if retries == 0:
        raise Exception("Kafka Connect is not available after waiting")
    
    # Load connector configuration
    config_path = './docker_all/config/config_debezium.json'
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")
    
    with open(config_path, 'r') as f:
        connector_config = json.load(f)
    
    try:
        # Register connector
        response = requests.post(
            'http://localhost:8083/connectors',
            headers={'Content-Type': 'application/json'},
            data=json.dumps(connector_config)
        )
        
        if response.status_code == 201:
            print("Connector registered successfully")
            # Check connector status
            time.sleep(KAFKA_SLEEP)  # Wait for connector to start
            status_response = requests.get('http://localhost:8083/connectors/postgres-connector/status')
            print(f"Connector status: {status_response.json()}")
        else:
            print(f"Failed to register connector: {response.text}")
    except requests.exceptions.ConnectionError as e:
        print(f"Failed to connect to Kafka Connect: {e}")
        raise

if __name__ == "__main__":
    register_connector()