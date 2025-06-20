"""MQTT Actions API for automating MQTT interactions."""
import json
import logging

from datetime import datetime
from typing import Callable, Optional, Union
from mqttactions.runtime import add_subscriber, get_client
from mqttactions.statemachine import StateMachine


logger = logging.getLogger(__name__)


class Watch:
    """Watches a topic for new messages and stores the most recent one.

    Example:
        light_state = Watch("some-light/state")
        print(light_state.string) # Interprets messages as UTF8 strings
    """
    _topic: str = ""
    _raw_value: Optional[bytes] = None
    _last_update: Optional[datetime] = None

    def __init__(self, topic: str):
        self._topic = topic
        add_subscriber(topic, self._on_message)

    def _on_message(self, payload: bytes):
        self._raw_value = payload
        self._last_update = datetime.now()

    @property
    def last_update(self) -> Optional[datetime]:
        return self._last_update

    @property
    def string(self) -> Optional[str]:
        return self._raw_value.decode('utf8') if self._raw_value else None


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
