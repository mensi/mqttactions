"""
Example action script for turning on the nightlight at sunset.
"""

from datetime import timedelta, time
from mqttactions import GeoLocation, publish

home = GeoLocation(47.3769, 8.5416)
LIGHT_COMMAND_TOPIC = "zigbee2mqtt/nightlight/set"


@home.on_sunset(offset=timedelta(minutes=10))
def turn_on_nightlight():
    publish(LIGHT_COMMAND_TOPIC, {'state': 'ON'})


@home.on_sunrise
def turn_off_nightlight():
    publish(LIGHT_COMMAND_TOPIC, {'state': 'OFF'})


# On startup, trigger the appropriate mode
if home.is_day():
    turn_off_nightlight()
else:
    turn_on_nightlight()
