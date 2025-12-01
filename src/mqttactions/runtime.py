import inspect
import json
import logging
import threading

from typing import Any, Callable, Dict, List, Optional, Union, TypedDict, get_origin
import paho.mqtt.client as mqtt
from mqttactions.payloadconversion import converter_by_type

logger = logging.getLogger(__name__)

# Web UI manager (optional, only set when web interface is enabled)
_web_manager = None


def set_web_manager(manager):
    """Register the web manager for broadcasting updates to the web UI.

    This is called by the web module when the web interface is started.
    """
    global _web_manager
    _web_manager = manager


def get_web_manager():
    """Get the web manager if it exists."""
    return _web_manager


class Subscriber(TypedDict):
    callback: Callable
    datatype: Optional[type]
    payload_filter: Optional[Any]


class SubscriberManager:
    subscribers_by_type: Dict[type, List[Subscriber]]

    def __init__(self) -> None:
        self.subscribers_by_type = {t: [] for t in converter_by_type.keys()}

    def add_subscriber(self, callback: Callable, payload_filter: Optional[Any] = None):
        callback_type: Optional[type] = None
        filter_type = payload_filter.__class__ if payload_filter is not None else None

        # Try to infer the type of argument this callback expects.
        params = inspect.signature(callback).parameters
        if len(params) > 1:
            logger.error(f"Subscriber {callback.__name__} takes {len(params)} arguments only 1 expected. Ignoring...")
            return
        if len(params) == 1:
            argtype = next(iter(params.values())).annotation
            if argtype is inspect._empty:
                # No type annotation, just give it the raw payload then...
                callback_type = bytes
            else:
                callback_type = argtype

        # Normalize any type annotations from typing
        if get_origin(callback_type) is not None:
            callback_type = get_origin(callback_type)

        # The payload filter and callback type must match
        if payload_filter is not None and callback_type is not None and payload_filter.__class__ is not callback_type:
            logger.error(f"Subscriber {callback.__name__} has incompatible payload filter and expected argument type.")
            return

        effective_type = callback_type or filter_type or bytes
        if effective_type not in self.subscribers_by_type:
            logger.error(f"Subscriber {callback.__name__} has an unsupported argument type {effective_type}.")
            return
        self.subscribers_by_type[effective_type].append({
            'callback': callback,
            'datatype': callback_type,
            'payload_filter': payload_filter,
        })

    def notify(self, payload: bytes):
        for datatype, subscribers in self.subscribers_by_type.items():
            if not subscribers:
                continue

            try:
                converted_payload = converter_by_type[datatype](payload)
            except Exception as e:
                logger.error(f"Unable to convert payload to {datatype}: {e}")
                continue

            for subscriber in subscribers:
                if subscriber['payload_filter'] is not None and converted_payload != subscriber['payload_filter']:
                    continue
                if subscriber['datatype'] is None:
                    subscriber['callback']()
                else:
                    subscriber['callback'](converted_payload)


# The client to be used by runtime functions
_mqtt_client: Optional[mqtt.Client] = None
# A dict mapping from a topic to subscribers to that topic
_subscribers: Dict[str, SubscriberManager] = {}
# Lock to protect concurrent access to _subscribers from callbacks
_subscribers_lock = threading.Lock()


def _on_mqtt_message(client, userdata, msg):
    """Process incoming MQTT messages and dispatch to registered handlers."""
    logger.debug(f"Received message on {msg.topic}: {msg.payload}")
    
    # Broadcast to web UI if available
    if _web_manager:
        try:
            payload_str = msg.payload.decode('utf-8')
        except:
            payload_str = str(msg.payload)
        _web_manager.broadcast(json.dumps({
            'type': 'mqtt_message',
            'data': {
                'topic': msg.topic,
                'payload': payload_str
            }
        }))
    
    with _subscribers_lock:
        if msg.topic not in _subscribers:
            logger.warning(f"Received message on {msg.topic} but no subscribers are registered.")
            return
        subscriber_manager = _subscribers[msg.topic]
    
    # Call notify outside the lock to avoid holding the lock during callback execution
    subscriber_manager.notify(msg.payload)


def _on_mqtt_connect(client, userdata, connect_flags: mqtt.ConnectFlags,
                     reason_code: mqtt.ReasonCode, properties: mqtt.Properties):
    """Connection callback."""
    if reason_code.is_failure:
        logger.error(f"MQTT connection failed: {reason_code.getName()}")
        return
    logger.info("MQTT connected")
    
    # Get a snapshot of topics while holding the lock
    with _subscribers_lock:
        topics = list(_subscribers.keys())
    
    # Subscribe to all topics
    for topic in topics:
        client.subscribe(topic)
        logger.info(f"Subscribed to topic: {topic}")


def _on_mqtt_disconnect(client, userdata, disconnect_flags, reason_code: mqtt.ReasonCode, properties):
    """Disconnection callback."""
    if reason_code.is_failure:
        logger.error(f"MQTT disconnected: {reason_code.getName()}")
    else:
        logger.info("MQTT disconnected")


def register_client(client: mqtt.Client):
    global _mqtt_client
    _mqtt_client = client
    _mqtt_client.on_message = _on_mqtt_message
    _mqtt_client.on_connect = _on_mqtt_connect
    _mqtt_client.on_disconnect = _on_mqtt_disconnect


def get_client() -> mqtt.Client:
    if _mqtt_client is None:
        raise Exception("No client was registered. Please make sure to call register_client")
    return _mqtt_client


def add_subscriber(topic: str, callback: Callable, payload_filter: Optional[Union[str, dict]] = None):
    with _subscribers_lock:
        if topic in _subscribers:
            _subscribers[topic].add_subscriber(callback, payload_filter)
            return
        logger.info(f"Subscribed to topic: {topic}")
        _subscribers[topic] = SubscriberManager()
        _subscribers[topic].add_subscriber(callback, payload_filter)
    get_client().subscribe(topic)


def get_subscribed_topics() -> List[str]:
    """Get a list of all topics that have subscribers.

    Returns:
        A list of MQTT topic strings
    """
    with _subscribers_lock:
        return list(_subscribers.keys())
