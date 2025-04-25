# test_0_register_debezium.py
import unittest
from unittest.mock import patch, MagicMock
import requests
import scripts.register_debezium as register_debezium
from scripts.register_debezium import KAFKA_CONNECT_URL, CONNECTOR_NAME
import os
import json

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) # Absolute path to the scripts directory
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR) # Absolute path to parent directory of scripts
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'docker_all', 'config', 'config_debezium.json')
CONFIG_PATH_TMP = os.path.join(PROJECT_ROOT, 'docker_all', 'config', 'temp_config_debezium.json')

class TestRegisterDebezium(unittest.TestCase):
    @patch('register_debezium.requests.get')
    def test_check_connector_exists_success(self, mock_get):
        """Test when the connector exists (status code 200)."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        self.assertTrue(register_debezium.check_connector_exists())
        mock_get.assert_called_once_with(f'{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}')

    @patch('register_debezium.requests.get')
    def test_check_connector_exists_failure(self, mock_get):
        """Test when the connector doesn't exist (status code not 200)."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        self.assertFalse(register_debezium.check_connector_exists())
        mock_get.assert_called_once_with(f'{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}')

    @patch('register_debezium.requests.get')
    def test_check_connector_exists_exception(self, mock_get):
        """Test when requests.get raises an exception."""
        mock_get.side_effect = requests.exceptions.RequestException("Connection error")
        self.assertFalse(register_debezium.check_connector_exists())
        mock_get.assert_called_once_with(f'{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}')

    @patch('register_debezium.requests.get')
    def test_check_kafka_connect_health_success(self, mock_get):
        """Test when Kafka Connect is healthy (status code 200)."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        self.assertTrue(register_debezium.check_kafka_connect_health())
        mock_get.assert_called_once_with(KAFKA_CONNECT_URL)

    @patch('register_debezium.requests.get')
    def test_check_kafka_connect_health_failure(self, mock_get):
        """Test when Kafka Connect is unhealthy (status code not 200)."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response
        self.assertFalse(register_debezium.check_kafka_connect_health())
        mock_get.assert_called_once_with(KAFKA_CONNECT_URL)

    @patch('register_debezium.requests.get')
    def test_check_kafka_connect_health_exception(self, mock_get):
        """Test when requests.get raises an exception."""
        mock_get.side_effect = requests.exceptions.RequestException("Connection error")
        self.assertFalse(register_debezium.check_kafka_connect_health())
        mock_get.assert_called_once_with(KAFKA_CONNECT_URL)

    @patch('register_debezium.check_connector_exists')
    @patch('register_debezium.requests.get')
    def test_register_connector_already_exists(self, mock_get, mock_check_exists):
        """Test when the connector is already registered."""
        mock_check_exists.return_value = True
        mock_response = MagicMock()
        mock_response.json.return_value = {'status': 'RUNNING'}
        mock_get.return_value = mock_response

        with self.assertRaises(SystemExit) as context:
            register_debezium.register_connector()

        mock_get.assert_called_once_with(f'{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/status')
        self.assertEqual(context.exception.code, 0)

    @patch('register_debezium.check_connector_exists')
    @patch('register_debezium.check_kafka_connect_health')
    @patch('register_debezium.requests.post')
    @patch('register_debezium.requests.get')
    def test_register_connector_success(self, mock_get, mock_post, mock_check_health, mock_check_exists):
        """Test successful registration of the connector."""
        mock_check_exists.return_value = False
        mock_check_health.return_value = True

        mock_post_response = MagicMock()
        mock_post_response.status_code = 201
        mock_post.return_value = mock_post_response

        mock_get_response = MagicMock()
        mock_get_response.json.return_value = {'status': 'RUNNING'}
        mock_get.return_value = mock_get_response

        # Create a temporary config file
        config_data = {"name": "postgres-connector", "config": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector"}}
        with open(CONFIG_PATH, 'w') as f:
            json.dump(config_data, f)

        try:
             register_debezium.register_connector()
        except Exception as e:
            self.fail(f"register_connector raised an exception: {e}")

        mock_post.assert_called_once()
        mock_get.assert_called_with(f'{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/status')

        # Clean up the temporary config file
        os.remove(CONFIG_PATH)

    @patch('register_debezium.check_connector_exists')
    @patch('register_debezium.check_kafka_connect_health')
    @patch('register_debezium.requests.post')
    def test_register_connector_failure(self, mock_post, mock_check_health, mock_check_exists):
        """Test failed registration of the connector."""
        mock_check_exists.return_value = False
        mock_check_health.return_value = True

        mock_post_response = MagicMock()
        mock_post_response.status_code = 500
        mock_post_response.text = "Internal Server Error"
        mock_post.return_value = mock_post_response

        # Create a temporary config file
        config_data = {"name": "postgres-connector", "config": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector"}}
        with open(CONFIG_PATH, 'w') as f:
            json.dump(config_data, f)
        try:
            register_debezium.register_connector()
        except Exception:
            pass

        mock_post.assert_called_once()

        # Clean up the temporary config file
        os.remove(CONFIG_PATH)

    @patch('register_debezium.check_connector_exists')
    @patch('register_debezium.check_kafka_connect_health')
    @patch('register_debezium.requests.post')
    def test_register_connector_connection_error(self, mock_post, mock_check_health, mock_check_exists):
        """Test connection error during registration."""
        mock_check_exists.return_value = False
        mock_check_health.return_value = True

        mock_post.side_effect = requests.exceptions.ConnectionError("Connection refused")

        # Create a temporary config file
        config_data = {"name": "postgres-connector", "config": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector"}}
        with open(CONFIG_PATH, 'w') as f:
            json.dump(config_data, f)
        with self.assertRaises(requests.exceptions.ConnectionError):
            register_debezium.register_connector()

        mock_post.assert_called_once()

        # Clean up the temporary config file
        os.remove(CONFIG_PATH)

    @patch('register_debezium.check_connector_exists')
    @patch('register_debezium.check_kafka_connect_health')
    @patch('register_debezium.requests.post')
    def test_register_connector_kafka_not_ready(self, mock_post, mock_check_health, mock_check_exists):
        """Test when Kafka Connect is not ready after waiting."""
        mock_check_exists.return_value = False
        mock_check_health.return_value = False

        with self.assertRaises(Exception) as context:
            register_debezium.register_connector()

        self.assertEqual(str(context.exception), "Kafka Connect is not available after waiting")

    @patch('register_debezium.check_connector_exists')
    def test_register_connector_config_file_not_found(self, mock_check_exists):
        """Test when the config file is not found."""
        mock_check_exists.return_value = False
        # Rename the file to simulate it not being found
        original_path = CONFIG_PATH
        temp_path = CONFIG_PATH_TMP

        # Ensure the config file exists before renaming
        if os.path.exists(original_path):
            os.rename(original_path, temp_path)

            try:
                with self.assertRaises(FileNotFoundError) as context:
                    register_debezium.register_connector()
                self.assertTrue("Config file not found" in str(context.exception))
            finally:
                # Rename the file back, regardless of whether the test passed or failed
                os.rename(temp_path, original_path)
        else:
            print("Config file not found, ensure it exists")


if __name__ == '__main__':
    unittest.main()