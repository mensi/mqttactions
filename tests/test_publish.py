import json
from mqttactions import publish


def test_publish_string_payload(mqtt_client):
    """Test publishing a string payload to a topic"""
    # Call the publish function with a string payload
    publish("test/topic", "test_message")

    # Check that the client's publish method was called with the correct arguments
    mqtt_client.publish.assert_called_once_with("test/topic", "test_message")


def test_publish_dict_payload(mqtt_client):
    """Test publishing a dict payload to a topic (should be converted to JSON)"""
    # Call the publish function with a dict payload
    test_dict = {"state": "ON", "brightness": 255}
    publish("test/topic", test_dict)

    # Check that the client's publish method was called with the dict converted to JSON
    expected_json = json.dumps(test_dict)
    mqtt_client.publish.assert_called_once_with("test/topic", expected_json)


def test_publish_empty_dict(mqtt_client):
    """Test publishing an empty dict payload"""
    # Call the publish function with an empty dict
    publish("test/topic", {})

    # Check that the client's publish method was called with the empty dict converted to JSON
    mqtt_client.publish.assert_called_once_with("test/topic", "{}")


def test_publish_complex_nested_dict(mqtt_client):
    """Test publishing a complex nested dict payload"""
    # Call the publish function with a complex nested dict
    complex_dict = {
        "state": "ON",
        "attributes": {
            "brightness": 255,
            "color": {
                "r": 255,
                "g": 0,
                "b": 0
            }
        },
        "options": ["dim", "flash", "solid"]
    }
    publish("test/topic", complex_dict)

    # Check that the client's publish method was called with the dict converted to JSON
    expected_json = json.dumps(complex_dict)
    mqtt_client.publish.assert_called_once_with("test/topic", expected_json)


def test_publish_multiple_messages(mqtt_client):
    """Test publishing multiple messages in succession"""
    # Publish multiple messages
    publish("test/topic1", "message1")
    publish("test/topic2", "message2")
    publish("test/topic3", {"value": 42})

    # Check that the client's publish method was called for each message
    assert mqtt_client.publish.call_count == 3

    # Check the individual calls
    calls = mqtt_client.publish.call_args_list
    assert calls[0][0] == ("test/topic1", "message1")
    assert calls[1][0] == ("test/topic2", "message2")
    assert calls[2][0] == ("test/topic3", json.dumps({"value": 42}))
