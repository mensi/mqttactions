from datetime import datetime
from mqttactions import Watch


def test_watch_initialization(mqtt_client):
    """Test that Watch properly subscribes to topics"""
    watch = Watch("test/topic")

    # Verify that the client was used to subscribe to the topic
    mqtt_client.subscribe.assert_called_once_with("test/topic")


def test_watch_receives_message(mqtt_client):
    """Test that Watch properly handles and stores incoming messages"""
    watch = Watch("test/topic")

    # Initially, the watch should not have any value
    assert watch._raw_value is None
    assert watch.last_update is None
    assert watch.string is None

    # Simulate receiving a message
    mqtt_client.simulate_message("test/topic", "test_message")

    # Verify that the message was stored
    assert watch._raw_value == b"test_message"
    assert isinstance(watch.last_update, datetime)
    assert watch.string == "test_message"


def test_watch_decodes_string_property(mqtt_client):
    """Test that the string property correctly decodes bytes to string"""
    watch = Watch("test/topic")

    # Simulate receiving a binary message
    mqtt_client.simulate_message("test/topic", b"binary\x00data")

    # Verify that the string property properly decodes the bytes
    assert watch._raw_value == b"binary\x00data"
    assert watch.string == "binary\x00data"


def test_watch_handles_unicode(mqtt_client):
    """Test that Watch properly handles and decodes Unicode strings"""
    watch = Watch("test/topic")

    # Simulate receiving a Unicode message
    mqtt_client.simulate_message("test/topic", "unicode: 你好")

    # Verify that the Unicode string is properly stored and decoded
    assert watch.string == "unicode: 你好"


def test_watch_multiple_instances(mqtt_client):
    """Test that multiple Watch instances can coexist and handle their own topics"""
    watch1 = Watch("test/topic1")
    watch2 = Watch("test/topic2")

    # Simulate receiving messages on different topics
    mqtt_client.simulate_message("test/topic1", "message1")
    mqtt_client.simulate_message("test/topic2", "message2")

    # Verify that each watch received its own message
    assert watch1.string == "message1"
    assert watch2.string == "message2"

    # Update one of the watches and verify the other is unchanged
    mqtt_client.simulate_message("test/topic1", "updated1")
    assert watch1.string == "updated1"
    assert watch2.string == "message2"
