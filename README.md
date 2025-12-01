# mqttactions

mqttactions is a simple CLI to build simple interactive behavior with MQTT. It's
particularly useful for build simple rules for home automation without going
to full in on a complex solution like Home Assistant or similar.

For example, mqttactions makes it super simple to listen to an MQTT topic
from a switch (e.g., from Zigbee2MQTT) and then publish a message to a different
topic to, for example, turn on a light.

Since the automation scripts are just Python, you can do arbitrarily complex
things. Want to have a button (via Zigbee2MQTT) that checks if you have a new 
email and if so, flashes a light 3 times? No problem, just requires some more
Python code.

Note that this project is my experimentation with AI-assisted coding. I do look
over every change, but I let AI do as much of the work as possible as long
as the result is still useful for my own usage.

## Installation

mqttactions is available on PyPI and can be installed with `pip`:

```console
$ pip install mqttactions
```

Especially if you plan to use complex scripts that require additional
packages, it's advisable to install mqttactions in a virtualenv.

## Usage

MQTT Actions provides two main commands:

### Discover MQTT Devices

Discover devices exposed via the Home Assistant MQTT discovery protocol:

```console
$ mqttactions --host mqtt.example.com discover
```

This can be used to help figure out what the command and/or state
topics of your devices are if they support Home Assistant's MQTT
discovery protocol.

Use the `--filter` option to do a substring match on any of the fields
in case you have many devices.

### Run Automation Scripts

Run automation scripts that respond to MQTT messages:

```console
$ mqttactions --host mqtt.example.com run script1.py script2.py
```

The scripts you pass are imported by the CLI which then runs until
you terminate it. Scripts can subscribe to topics and publish messages
but also anything else an ordinary Python module could do.

## Writing Automation Scripts

Scripts use a simple decorator-based API for reacting to MQTT messages:

```python
from mqttactions import on, publish

@on("some-switch/action", payload="press_1")
def turn_on_light():
    publish("some-light/set", {"state": "ON"})

@on("some-switch/action", payload="press_2")
def turn_off_light():
    publish("some-light/set", {"state": "OFF"})
```

Check the `examples/` directory for more example scripts that demonstrate
common automation patterns.

The API is currently a very rough first attempt, expect it to evolve in breaking
ways for some time.
