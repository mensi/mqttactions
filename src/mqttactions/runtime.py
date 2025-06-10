import inspect
import json
import logging

from typing import Any, Callable, Dict, List, Optional, Union, TypedDict, get_origin
import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


class Subscriber(TypedDict):
    callback: Callable
    datatype: Optional[type]
    payload_filter: Optional[Any]


class SubscriberManager:
    subscribers_by_type: Dict[type, List[Subscriber]]
    converters: Dict[type, Callable]

    def __init__(self) -> None:
        self.subscribers_by_type = {}
        self.converters = {}

        # The idea here is to make it easy to add new automatically converted types,
        # so you can just define a new method named _convert_to_TYPE to support a new one.
        # -> find all converter functions.
        for name, method in inspect.getmembers(self, predicate=inspect.isfunction):
            if name.startswith('_convert_to_'):
                return_type = method.__annotations__['return']
                assert name == f'_convert_to_{return_type.__name__}', "Incorrectly named converter method."
                self.subscribers_by_type[return_type] = []
                self.converters[return_type] = method

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
                converted_payload = self.converters[datatype](payload)
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

    @staticmethod
    def _convert_to_bytes(payload: bytes) -> bytes:
        return payload

    @staticmethod
    def _convert_to_str(payload: bytes) -> str:
        return payload.decode('utf8')

    @staticmethod
    def _convert_to_dict(payload: bytes) -> dict:
        return json.loads(payload)


# The client to be used by runtime functions
_mqtt_client: Optional[mqtt.Client] = None
# A dict mapping from a topic to subscribers to that topic
_subscribers: Dict[str, SubscriberManager] = {}


def _on_mqtt_message(client, userdata, msg):
    """Process incoming MQTT messages and dispatch to registered handlers."""
    logger.debug(f"Received message on {msg.topic}: {msg.payload}")
    if msg.topic not in _subscribers:
        logger.warning(f"Received message on {msg.topic} but no subscribers are registered.")
        return
    _subscribers[msg.topic].notify(msg.payload)


def register_client(client: mqtt.Client):
    global _mqtt_client
    _mqtt_client = client
    _mqtt_client.on_message = _on_mqtt_message


def get_client() -> mqtt.Client:
    if _mqtt_client is None:
        raise Exception("No client was registered. Please make sure to call register_client")
    return _mqtt_client


def add_subscriber(topic: str, callback: Callable, payload_filter: Optional[Union[str, dict]] = None):
    if topic not in _subscribers:
        get_client().subscribe(topic)
        logger.info(f"Subscribed to topic: {topic}")
        _subscribers[topic] = SubscriberManager()
    _subscribers[topic].add_subscriber(callback, payload_filter)
