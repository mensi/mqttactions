"""
Example mqttactions script that emulates a motion nightlight with Frigate.

This is similar to the frigate_motion.py example, but instead of turning a
light on or off depending on whether people are detected, it changes the
brightness of a nightlight.
"""

from datetime import timedelta, time
from mqttactions import StateMachine, publish, GeoLocation
import logging

logger = logging.getLogger(__name__)

CAMERA_NAME = "backyard"
LIGHT_NAME = "floodlight"

MOTION_TOPIC = f"frigate/{CAMERA_NAME}/person/active"
LIGHT_STATE_TOPIC = f"zigbee2mqtt/{LIGHT_NAME}"
LIGHT_COMMAND_TOPIC = f"zigbee2mqtt/{LIGHT_NAME}/set"
NIGHTLIGHT_OFF = time(23, 0)
LINGER_TIMEOUT_SECONDS = 5

location = GeoLocation(47.3769, 8.5417)
sm = StateMachine()

# Set up the states of our machine.
st_off = sm.add_state('Off')
st_on = sm.add_state('On')
st_nightlight_on = sm.add_state('NightlightOn')
st_motion = sm.add_state('Motion')
st_linger = sm.add_state('Linger')


# Go to the on-state when the light turns on from an external source.
@st_off.on_message_filtered(LIGHT_STATE_TOPIC, st_on)
def light_state_is_on(payload: dict):
    """Called when the light turns on."""
    return payload.get('state') == 'ON'


# Go to the off-state when the light turns off from an external source.
@st_on.on_message_filtered(LIGHT_STATE_TOPIC, st_off)
@st_nightlight_on.on_message_filtered(LIGHT_STATE_TOPIC, st_off)
@st_motion.on_message_filtered(LIGHT_STATE_TOPIC, st_off)
@st_linger.on_message_filtered(LIGHT_STATE_TOPIC, st_off)
def light_state_is_off(payload: dict):
    """Called when the light turns on."""
    return payload.get('state') == 'OFF'


# If the sun sets, got to nightlight mode.
@location.on_sunset()
def sunset():
    sm.transition_to(st_nightlight_on)


# In nightlight mode, turn the light on, low brightness
@st_nightlight_on.on_entry
def nightlight_on():
    publish(LIGHT_COMMAND_TOPIC, {'state': 'ON', 'brightness': 15})


# If we reach the nightlight cutoff time, transition to off
@location.on_localtime(NIGHTLIGHT_OFF)
def nightlight_cutoff():
    if sm.get_current_state() == st_nightlight_on:
        sm.transition_to(st_off)


# If we're off, in nightlight or in linger, and people are detected, go to the motion state.
@st_off.on_message_filtered(MOTION_TOPIC, st_motion)
@st_nightlight_on.on_message_filtered(MOTION_TOPIC, st_motion)
@st_linger.on_message_filtered(MOTION_TOPIC, st_motion)
def people_are_detected(payload: int):
    since_sunset = location.time_since_sunset()
    if since_sunset < timedelta(minutes=30):
        logging.info(f"Not past sunset: {since_sunset}")
        return False
    return payload > 0


# In the motion state, turn the light on, high brightness
@st_motion.on_entry
def on_motion_entry():
    """Called upon motion detection."""
    publish(LIGHT_COMMAND_TOPIC, {'state': 'ON', 'brightness': 230})


# Once no more people are detected, go to the linger state.
@st_motion.on_message_filtered(MOTION_TOPIC, st_linger)
def no_people_are_detected(payload: int):
    return payload == 0


# After a timeout, go back to the appropriate state.
def idle_state():
    if location.is_after_sunset() and location.localtime() < NIGHTLIGHT_OFF:
        return st_nightlight_on
    else:
        return st_off


st_linger.after_timeout(LINGER_TIMEOUT_SECONDS, idle_state)


@st_off.on_entry
def on_off_entry():
    """Called when the system is turning the light off."""
    publish(LIGHT_COMMAND_TOPIC, {'state': 'OFF'})


# On startup, go to the appropriate state.
sm.transition_to(idle_state())
