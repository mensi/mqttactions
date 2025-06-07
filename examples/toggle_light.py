"""
Example mqttactions script that toggles a light when a button is pressed.

Usage:
    mqttactions --host mqtt.example.com run toggle_light.py
"""

import logging
from mqttactions import on, publish

logger = logging.getLogger(__name__)


# This function will be called when the "press_1" action is received from the switch
@on("some-switch/action", payload="press_1")
def turn_on_light():
    logger.info("Button press detected, turning on light")
    publish("some-light/set", {"state": "ON"})


# This function will be called when the "press_2" action is received from the switch
@on("some-switch/action", payload="press_2")
def turn_off_light():
    logger.info("Button press detected, turning off light")
    publish("some-switch/set", {"state": "OFF"})


logger.info("Toggle light script loaded")
