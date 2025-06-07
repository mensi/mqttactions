import inspect
import json
import logging

from typing import Callable, Dict, List, Optional, Union, TypedDict, get_origin
import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


class Subscriber(TypedDict):
    callback: Callable
    datatype: Optional[type]
    payload_filter: Optional[Union[str, dict]]


class Subscribers(TypedDict):
    raw_subscribers: List[Subscriber]
    str_subscribers: List[Subscriber]
    dict_subscribers: List[Subscriber]


# The client to be used by runtime functions
_mqtt_client: Optional[mqtt.Client] = None
# A dict mapping from a topic to subscribers to that topic
_subscribers: Dict[str, Subscribers] = {}


def _notify_subscribers(subscribers: List[Subscriber], msg, transform: Callable = lambda x: x):
    if not subscribers:
        return

    try:
        decoded_payload = transform(msg.payload)
    except Exception as e:
        logger.error(f"Error decoding MQTT message to {msg.topic}: {e}")
        return

    for sub in subscribers:
        if sub['payload_filter'] is not None and decoded_payload != sub['payload_filter']:
            continue
        try:
            if sub['datatype'] is None:
                sub['callback']()
            else:
                sub['callback'](decoded_payload)
        except Exception as e:
            logger.error(f"Error executing MQTT handler {sub['callback'].__name__} for topic {msg.topic}: {e}")


def _on_mqtt_message(client, userdata, msg):
    """Process incoming MQTT messages and dispatch to registered handlers."""
    logger.debug(f"Received message on {msg.topic}: {msg.payload}")
    if msg.topic not in _subscribers:
        logger.warning(f"Received message on {msg.topic} but no subscribers are registered.")
        return

    subscribers = _subscribers[msg.topic]
    _notify_subscribers(subscribers['raw_subscribers'], msg)
    _notify_subscribers(subscribers['str_subscribers'], msg, lambda x: x.decode('utf-8'))
    _notify_subscribers(subscribers['dict_subscribers'], msg, lambda x: json.loads(x.decode('utf-8')))


def register_client(client: mqtt.Client):
    global _mqtt_client
    _mqtt_client = client
    _mqtt_client.on_message = _on_mqtt_message


def get_client() -> mqtt.Client:
    if _mqtt_client is None:
        raise Exception("No client was registered. Please make sure to call register_client")
    return _mqtt_client


def add_subscriber(topic: str, callback: Callable, payload_filter: Optional[Union[str, dict]] = None):
    subscriber: Subscriber = {
        'callback': callback,
        'datatype': None,
        'payload_filter': payload_filter,
    }
    list_flavor = 'raw_subscribers'

    # If we have a payload filter, the datatype should be that
    if payload_filter is not None:
        if isinstance(payload_filter, str):
            list_flavor = 'str_subscribers'
        elif isinstance(payload_filter, dict):
            list_flavor = 'dict_subscribers'
        else:
            logger.warning(f"Subscriber {callback.__name__} has an unsupported payload filter type.")

    # Try to infer the type of argument this callback expects.
    params = inspect.signature(callback).parameters
    if len(params) > 1:
        logger.error(f"Subscriber {callback.__name__} takes {len(params)} arguments only 1 expected. Ignoring...")
    elif len(params) == 1:
        argtype = next(iter(params.values())).annotation

        subscriber["datatype"] = bytes
        if argtype is str:
            subscriber["datatype"] = str
            list_flavor = 'str_subscribers'
        elif argtype is dict or get_origin(argtype) is dict:
            subscriber["datatype"] = dict
            list_flavor = 'dict_subscribers'
        else:
            logger.warning(f"Subscriber {callback.__name__} has an unsupported argument type {argtype}.")

    if topic in _subscribers:
        _subscribers[topic][list_flavor].append(subscriber)
    else:
        _subscribers[topic] = {
            "raw_subscribers": [],
            "str_subscribers": [],
            "dict_subscribers": [],
        }
        _subscribers[topic][list_flavor].append(subscriber)
        get_client().subscribe(topic)
        logger.info(f"Subscribed to topic: {topic}")


