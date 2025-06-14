import pytest
from unittest.mock import MagicMock
from mqttactions import runtime


class MockMQTTClient:
    def __init__(self):
        self.on_message = None
        self.subscribe = MagicMock()
        self.publish = MagicMock()

    def simulate_message(self, topic, payload, qos=0, retain=False):
        """Simulate receiving a message from the MQTT broker"""
        if self.on_message:
            # Create a message object similar to what paho-mqtt would provide
            msg = MagicMock()
            msg.topic = topic
            # Convert string payloads to bytes as the real client would do
            if isinstance(payload, str):
                msg.payload = payload.encode('utf-8')
            elif isinstance(payload, dict):
                import json
                msg.payload = json.dumps(payload).encode('utf-8')
            else:
                msg.payload = payload
            msg.qos = qos
            msg.retain = retain

            # Call the registered callback
            self.on_message(self, None, msg)


@pytest.fixture
def mqtt_client():
    """Create a mock MQTT client for testing"""
    # Create a mock client
    mock_client = MockMQTTClient()

    # Register it with the runtime
    runtime.register_client(mock_client)

    # Reset any subscribers that might have been registered in previous tests
    runtime._subscribers = {}

    # Return the mock client for use in tests
    return mock_client
