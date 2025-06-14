from unittest.mock import MagicMock
from mqttactions import on


def test_on_basic_subscription(mqtt_client):
    """Test that the @on decorator properly subscribes a function to a topic"""
    callback = MagicMock()

    @on("test/topic")
    def handle_message():
        callback()

    # Check that we've subscribed to the topic
    mqtt_client.subscribe.assert_called_once_with("test/topic")

    # Simulate a message on the topic
    mqtt_client.simulate_message("test/topic", "test_message")

    # Check that our callback was called
    callback.assert_called_once()


def test_on_with_string_payload_filter(mqtt_client):
    """Test that the @on decorator with a string payload filter works correctly"""
    callback = MagicMock()

    @on("test/topic", payload="specific_value")
    def handle_message():
        callback()

    # Simulate a message that doesn't match the filter
    mqtt_client.simulate_message("test/topic", "wrong_value")

    # The callback should not be called
    callback.assert_not_called()

    # Simulate a message that matches the filter
    mqtt_client.simulate_message("test/topic", "specific_value")

    # Now the callback should be called
    callback.assert_called_once()


def test_on_with_dict_payload_filter(mqtt_client):
    """Test that the @on decorator with a dict payload filter works correctly"""
    callback = MagicMock()

    @on("test/topic", payload={"state": "ON"})
    def handle_message():
        callback()

    # Simulate a message that doesn't match the filter
    mqtt_client.simulate_message("test/topic", {"state": "OFF"})

    # The callback should not be called
    callback.assert_not_called()

    # Simulate a message that matches the filter
    mqtt_client.simulate_message("test/topic", {"state": "ON"})

    # Now the callback should be called
    callback.assert_called_once()


def test_on_with_typed_callback(mqtt_client):
    """Test that the @on decorator correctly handles functions with type annotations"""
    string_callback = MagicMock()
    dict_callback = MagicMock()

    @on("test/string")
    def handle_string(message: str):
        string_callback(message)

    @on("test/dict")
    def handle_dict(message: dict):
        dict_callback(message)

    # Simulate receiving messages
    mqtt_client.simulate_message("test/string", "string_message")
    mqtt_client.simulate_message("test/dict", {"key": "value"})

    # Check that callbacks were called with correctly typed arguments
    string_callback.assert_called_once_with("string_message")
    dict_callback.assert_called_once_with({"key": "value"})


def test_on_multiple_subscribers(mqtt_client):
    """Test that multiple functions can subscribe to the same topic"""
    callback1 = MagicMock()
    callback2 = MagicMock()

    @on("test/topic")
    def handle_message1():
        callback1()

    @on("test/topic")
    def handle_message2():
        callback2()

    # The client should have subscribed to the topic only once
    mqtt_client.subscribe.assert_called_once_with("test/topic")

    # Simulate a message
    mqtt_client.simulate_message("test/topic", "test_message")

    # Both callbacks should have been called
    callback1.assert_called_once()
    callback2.assert_called_once()
