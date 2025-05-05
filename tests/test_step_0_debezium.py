import pytest
from unittest.mock import patch, MagicMock
import requests
import scripts.step_0_register_debezium as register_debezium
from scripts.step_0_register_debezium import KAFKA_CONNECT_URL, CONNECTOR_NAME
import os
import json
from scripts.utils import get_logger

# Configure logger
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
LOG_FILE = os.path.join(LOG_DIR, 'test_step_0_debezium.log')

log_file = f"/opt/airflow/logs/1_get_transaction_data/{RUN_DATE_STR}.log"
logger = get_logger(__name__, log_file)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'docker_all', 'config', 'config_debezium.json')
CONFIG_PATH_TMP = os.path.join(PROJECT_ROOT, 'docker_all', 'config', 'temp_config_debezium.json')

@pytest.fixture(autouse=True)
def setup_and_teardown():
    logger.info("Starting a new test case")
    yield
    logger.info("Finished the test case")


def test_check_connector_exists_success(caplog, monkeypatch):
    """Test when the connector exists (status code 200)."""
    logger.info("Testing check_connector_exists_success")
    mock_response = MagicMock()
    mock_response.status_code = 200
    monkeypatch.setattr('scripts.step_0_register_debezium.requests.get', MagicMock(return_value=mock_response))
    assert register_debezium.check_connector_exists() == True
    logger.info("check_connector_exists_success passed")


def test_check_connector_exists_failure(caplog, monkeypatch):
    """Test when the connector doesn't exist (status code not 200)."""
    logger.info("Testing check_connector_exists_failure")
    mock_response = MagicMock()
    mock_response.status_code = 404
    monkeypatch.setattr('scripts.step_0_register_debezium.requests.get', MagicMock(return_value=mock_response))
    assert register_debezium.check_connector_exists() == False
    logger.info("check_connector_exists_failure passed")


def test_check_connector_exists_exception(caplog, monkeypatch):
    """Test when requests.get raises an exception."""
    logger.info("Testing check_connector_exists_exception")
    monkeypatch.setattr('scripts.step_0_register_debezium.requests.get', MagicMock(side_effect=requests.exceptions.RequestException("Connection error")))
    assert register_debezium.check_connector_exists() == False
    logger.info("check_connector_exists_exception passed")


def test_check_kafka_connect_health_success(caplog, monkeypatch):
    """Test when Kafka Connect is healthy (status code 200)."""
    logger.info("Testing check_kafka_connect_health_success")
    mock_response = MagicMock()
    mock_response.status_code = 200
    monkeypatch.setattr('scripts.step_0_register_debezium.requests.get', MagicMock(return_value=mock_response))
    assert register_debezium.check_kafka_connect_health() == True
    logger.info("check_kafka_connect_health_success passed")


def test_check_kafka_connect_health_failure(caplog, monkeypatch):
    """Test when Kafka Connect is unhealthy (status code not 200)."""
    logger.info("Testing check_kafka_connect_health_failure")
    mock_response = MagicMock()
    mock_response.status_code = 500
    monkeypatch.setattr('scripts.step_0_register_debezium.requests.get', MagicMock(return_value=mock_response))
    assert register_debezium.check_kafka_connect_health() == False
    logger.info("check_kafka_connect_health_failure passed")


def test_check_kafka_connect_health_exception(caplog, monkeypatch):
    """Test when requests.get raises an exception."""
    logger.info("Testing check_kafka_connect_health_exception")
    monkeypatch.setattr('scripts.step_0_register_debezium.requests.get', MagicMock(side_effect=requests.exceptions.RequestException("Connection error")))
    assert register_debezium.check_kafka_connect_health() == False
    logger.info("check_kafka_connect_health_exception passed")


def test_register_connector_already_exists(caplog, monkeypatch):
    """Test when the connector is already registered."""
    logger.info("Testing register_connector_already_exists")
    monkeypatch.setattr('scripts.step_0_register_debezium.check_connector_exists', MagicMock(return_value=True))
    mock_response = MagicMock()
    mock_response.json.return_value = {'status': 'RUNNING'}
    monkeypatch.setattr('scripts.step_0_register_debezium.requests.get', MagicMock(return_value=mock_response))
    with pytest.raises(SystemExit) as context:
        register_debezium.register_connector()
    assert context.value.code == 0
    logger.info("register_connector_already_exists passed")


def test_register_connector_success(caplog, monkeypatch):
    """Test successful registration of the connector."""
    logger.info("Testing register_connector_success")
    monkeypatch.setattr('scripts.step_0_register_debezium.check_connector_exists', MagicMock(return_value=False))
    monkeypatch.setattr('scripts.step_0_register_debezium.check_kafka_connect_health', MagicMock(return_value=True))
    mock_post_response = MagicMock()
    mock_post_response.status_code = 201
    monkeypatch.setattr('scripts.step_0_register_debezium.requests.post', MagicMock(return_value=mock_post_response))
    mock_get_response = MagicMock()
    mock_get_response.json.return_value = {'status': 'RUNNING'}
    monkeypatch.setattr('scripts.step_0_register_debezium.requests.get', MagicMock(return_value=mock_get_response))
    # Create a temporary config file
    config_data = {"name": "postgres-connector", "config": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector"}}
    with open(CONFIG_PATH, 'w') as f:
        json.dump(config_data, f)
    try:
        register_debezium.register_connector()
    except Exception as e:
        assert False, f"register_connector raised an exception: {e}"
    # Clean up the temporary config file
    os.remove(CONFIG_PATH)
    logger.info("register_connector_success passed")


def test_register_connector_failure(caplog, monkeypatch):
    """Test failed registration of the connector."""
    logger.info("Testing register_connector_failure")
    monkeypatch.setattr('scripts.step_0_register_debezium.check_connector_exists', MagicMock(return_value=False))
    monkeypatch.setattr('scripts.step_0_register_debezium.check_kafka_connect_health', MagicMock(return_value=True))
    mock_post_response = MagicMock()
    mock_post_response.status_code = 500
    mock_post_response.text = "Internal Server Error"
    monkeypatch.setattr('scripts.step_0_register_debezium.requests.post', MagicMock(return_value=mock_post_response))
    # Create a temporary config file
    config_data = {"name": "postgres-connector", "config": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector"}}
    with open(CONFIG_PATH, 'w') as f:
        json.dump(config_data, f)
    try:
        register_debezium.register_connector()
    except Exception:
        pass
    # Clean up the temporary config file
    os.remove(CONFIG_PATH)
    logger.info("register_connector_failure passed")


def test_register_connector_connection_error(caplog, monkeypatch):
    """Test connection error during registration."""
    logger.info("Testing register_connector_connection_error")
    monkeypatch.setattr('scripts.step_0_register_debezium.check_connector_exists', MagicMock(return_value=False))
    monkeypatch.setattr('scripts.step_0_register_debezium.check_kafka_connect_health', MagicMock(return_value=True))
    monkeypatch.setattr('scripts.step_0_register_debezium.requests.post', MagicMock(side_effect=requests.exceptions.ConnectionError("Connection refused")))
    # Create a temporary config file
    config_data = {"name": "postgres-connector", "config": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector"}}
    with open(CONFIG_PATH, 'w') as f:
        json.dump(config_data, f)
    with pytest.raises(requests.exceptions.ConnectionError):
        register_debezium.register_connector()
    # Clean up the temporary config file
    os.remove(CONFIG_PATH)
    logger.info("register_connector_connection_error passed")


def test_register_connector_kafka_not_ready(caplog, monkeypatch):
    """Test when Kafka Connect is not ready after waiting."""
    logger.info("Testing register_connector_kafka_not_ready")
    monkeypatch.setattr('scripts.step_0_register_debezium.check_connector_exists', MagicMock(return_value=False))
    monkeypatch.setattr('scripts.step_0_register_debezium.check_kafka_connect_health', MagicMock(return_value=False))
    with pytest.raises(Exception, match="Kafka Connect is not available after waiting"):
        register_debezium.register_connector()
    logger.info("register_connector_kafka_not_ready passed")


def test_register_connector_config_file_not_found(caplog, monkeypatch):
    """Test when the config file is not found."""
    logger.info("Testing register_connector_config_file_not_found")
    monkeypatch.setattr('scripts.step_0_register_debezium.check_connector_exists', MagicMock(return_value=False))
    # Rename the file to simulate it not being found
    original_path = CONFIG_PATH
    temp_path = CONFIG_PATH_TMP
    # Ensure the config file exists before renaming
    if os.path.exists(original_path):
        os.rename(original_path, temp_path)
        try:
            with pytest.raises(FileNotFoundError) as context:
                register_debezium.register_connector()
            assert "Config file not found" in str(context.value)
        finally:
            # Rename the file back, regardless of whether the test passed or failed
            os.rename(temp_path, original_path)
    else:
        print("Config file not found, ensure it exists")
    logger.info("register_connector_config_file_not_found passed")