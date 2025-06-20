"""
Example mqttactions script that manages 2 lights with 4 buttons.

The basic idea is to have an indirect floor light and a ceiling light in
low- and high-brightness modes. Two simple on/off switches and two
larger 4-way switches are used to select different modes.

The small switches increase the number of lights and brightness with
each consecutive on-press. The big buttons have dedicated assignments
for the different modes. 

Usage:
    mqttactions --host mqtt.example.com run statemachine_lights.py
"""

from mqttactions import StateMachine, publish

sm = StateMachine()
both_off = sm.add_state('BothOff')
floor_light_on = sm.add_state('FloorLightOn')
ceiling_light_low = sm.add_state('CeilingLightLow')
ceiling_light_high = sm.add_state('CeilingLightHigh')


@both_off.on_entry
def turn_both_off():
    publish('floorlight/set', 'OFF')
    publish('ceilinglight/set', {'state': 'OFF'})


@floor_light_on.on_entry
def turn_floor_light_on():
    publish('floorlight/set', 'ON')
    publish('ceilinglight/set', {'state': 'OFF'})


@ceiling_light_low.on_entry
def turn_ceiling_light_low():
    publish('floorlight/set', 'ON')
    publish('ceilinglight/set', {'state': 'ON', 'brightness': 10})


@ceiling_light_high.on_entry
def turn_ceiling_light_high():
    publish('floorlight/set', 'ON')
    publish('ceilinglight/set', {'state': 'ON', 'brightness': 230})


# On button just steps up the chain
for a, b in [(both_off, floor_light_on), (floor_light_on, ceiling_light_low),
             (ceiling_light_low, ceiling_light_high)]:
    a.on_message('switch1/action', b, payload_filter='on')
    a.on_message('switch2/action', b, payload_filter='on')
    a.on_message('bigswitch1/action', b, payload_filter='on')
    a.on_message('bigswitch2/action', b, payload_filter='on')

# Off button in all states transitions to all off
for state in [floor_light_on, ceiling_light_low, ceiling_light_high]:
    state.on_message('switch1/action', both_off, payload_filter='off')
    state.on_message('switch2/action', both_off, payload_filter='off')
    state.on_message('bigswitch1/action', both_off, payload_filter='off')
    state.on_message('bigswitch2/action', both_off, payload_filter='off')


# The left button always goes to low
for state in [both_off, floor_light_on, ceiling_light_high]:
    state.on_message('bigswitch1/action', ceiling_light_low,
                     payload_filter='arrow_left_click')
    state.on_message('bigswitch2/action', ceiling_light_low,
                     payload_filter='arrow_left_click')

# The right button always goes to high
for state in [both_off, floor_light_on, ceiling_light_low]:
    state.on_message('bigswitch1/action', ceiling_light_high,
                     payload_filter='arrow_right_click')
    state.on_message('bigswitch2/action', ceiling_light_high,
                     payload_filter='arrow_right_click')

