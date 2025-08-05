"""
Example mqttactions script that emulates a motion light with Frigate.

Frigate NVR publishes the count of detected objects to a topic. For example,
for a camera named "backyard", it will publish the count of active person
detections to "frigate/backyard/person/active".

We can set up a state machine that turns a light on when the count goes above
zero. To make it a bit less jittery, we move to a linger state when the count
goes back to zero and move back to the off state after a timeout.

To not interfere with normal operation of the light, we do not transition to
the motion detected state if the light is already on.
"""

from datetime import timedelta
from mqttactions import StateMachine, publish, GeoLocation
import logging

logger = logging.getLogger(__name__)

CAMERA_NAME = "garden" #CAMERA_NAME = "backyard"
LIGHT_NAME = "kitchen/ceilinglight" #LIGHT_NAME = "floodlight"

MOTION_TOPIC = f"frigate/{CAMERA_NAME}/person/active"
LIGHT_STATE_TOPIC = f"zigbee2mqtt/{LIGHT_NAME}"
LIGHT_COMMAND_TOPIC = f"zigbee2mqtt/{LIGHT_NAME}/set"
LINGER_TIMEOUT_SECONDS = 15

location = GeoLocation(47.3769, 8.5417)
sm = StateMachine()

# Set up the states of our machine.
st_off = sm.add_state('Off')
st_on = sm.add_state('On')
st_motion = sm.add_state('Motion')
st_linger = sm.add_state('Linger')


# Go to the on-state when the light turns on from an external source.
@st_off.on_message_filtered(LIGHT_STATE_TOPIC, st_on)
def light_state_is_on(payload: dict):
    """Called when the light turns on."""
    return payload.get('state') == 'ON'


# Go to the off-state when the light turns off from an external source.
@st_on.on_message_filtered(LIGHT_STATE_TOPIC, st_off)
@st_motion.on_message_filtered(LIGHT_STATE_TOPIC, st_off)
@st_linger.on_message_filtered(LIGHT_STATE_TOPIC, st_off)
def light_state_is_off(payload: dict):
    """Called when the light turns on."""
    return payload.get('state') == 'OFF'


# If we're off or in linger, and people are detected, go to the motion state.
@st_off.on_message_filtered(MOTION_TOPIC, st_motion)
@st_linger.on_message_filtered(MOTION_TOPIC, st_motion)
def people_are_detected(payload: int):
    since_sunset = location.time_since_sunset()
    if since_sunset < timedelta(minutes=30):
        logging.info(f"Not enough time since sunset: {since_sunset}")
        return False
    return payload > 0


# In the motion state, turn the light on
@st_motion.on_entry
def on_motion_entry():
    """Called upon motion detection."""
    publish(LIGHT_COMMAND_TOPIC, {'state': 'ON'})


# Once no more people are detected, go to the linger state.
@st_motion.on_message_filtered(MOTION_TOPIC, st_linger)
def no_people_are_detected(payload: int):
    return payload == 0


# After a timeout, go back to off.
st_linger.after_timeout(LINGER_TIMEOUT_SECONDS, st_off)


@st_off.on_entry
def on_off_entry():
    """Called when the system is turning the light off."""
    publish(LIGHT_COMMAND_TOPIC, {'state': 'OFF'})
