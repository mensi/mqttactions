"""MQTT Actions API for automating MQTT interactions."""
import json
import logging

from typing import Callable, Optional, Union
from mqttactions.runtime import add_subscriber, get_client


logger = logging.getLogger(__name__)


def on(topic: str, payload: Optional[Union[str, dict]] = None) -> Callable:
    """Decorator to subscribe to an MQTT topic and execute a function when a matching message is received.

    Args:
        topic: The MQTT topic to subscribe to
        payload: Optional payload filter. If provided, the function will only be called when
                 the received payload matches this value. Can be a string or a dict.

    Returns:
        Decorator function

    Example:
        @on("some-switch/action", payload="press_1")
        def turn_on_light():
            publish("some-light/set", {"state": "ON"})
    """
    def decorator(func: Callable) -> Callable:
        add_subscriber(topic, func, payload)
        return func
    return decorator


def publish(topic: str, payload: Union[str, dict]) -> None:
    """Publish a message to an MQTT topic.

    Args:
        topic: The MQTT topic to publish to
        payload: The payload to publish, can be a string or a dict (will be converted to JSON)

    Example:
        publish("some-light/set", {"state": "ON"})
        publish("some-light/set", "ON")
    """
    # Convert dict payload to JSON string
    if isinstance(payload, dict):
        payload = json.dumps(payload)

    logger.debug(f"Publishing to {topic}: {payload}")
    get_client().publish(topic, payload)
