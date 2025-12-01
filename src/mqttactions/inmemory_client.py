import logging
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


class InMemoryMqttClient:
    """An in-memory MQTT client for testing and simulation."""
    def __init__(self, *args, **kwargs):
        self.on_message: Optional[Callable] = None
        self.on_connect: Optional[Callable] = None
        self.on_disconnect: Optional[Callable] = None
        self.subscriptions: List[str] = []
        self.connected = False
        self._userdata = None

    def username_pw_set(self, username, password):
        pass

    def connect(self, host, port=1883, keepalive=60):
        self.connected = True
        logger.info(f"InMemoryMqttClient connected to {host}:{port}")
        if self.on_connect:
            class ReasonCode:
                def getName(self): return "Success"
                @property
                def is_failure(self): return False
            
            self.on_connect(self, self._userdata, {}, ReasonCode(), None)

    def disconnect(self):
        self.connected = False
        logger.info("InMemoryMqttClient disconnected")
        if self.on_disconnect:
            # runtime._on_mqtt_disconnect expects: client, userdata, disconnect_flags, reason_code, properties
            class ReasonCode:
                def getName(self): return "Success"
                @property
                def is_failure(self): return False
            self.on_disconnect(self, self._userdata, {}, ReasonCode(), None)

    def subscribe(self, topic, qos=0):
        logger.info(f"InMemoryMqttClient subscribed to {topic}")
        if topic not in self.subscriptions:
            self.subscriptions.append(topic)
        return 0, 1

    def publish(self, topic, payload=None, qos=0, retain=False):
        logger.info(f"InMemoryMqttClient published to {topic}: {payload}")
        # In a real broker, this would go to the broker and then come back to subscribers.
        # Here, we can optionally loop it back if we want to simulate that behavior,
        # but for now we just log it. The 'inject' functionality will handle incoming messages.
        return None

    def loop_start(self):
        pass

    def loop_stop(self):
        pass
    
    def inject_message(self, topic: str, payload: str):
        """Simulate receiving a message from the broker."""
        if not self.on_message:
            logger.warning("No on_message callback registered")
            return

        logger.info(f"Injecting message: {topic} -> {payload}")
        
        class Message:
            def __init__(self, topic, payload):
                self.topic = topic
                self.payload = payload if isinstance(payload, bytes) else payload.encode('utf-8')

        msg = Message(topic, payload)
        self.on_message(self, self._userdata, msg)
