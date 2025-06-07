import click
import json
import logging
import time
from typing import Dict, List, TypedDict, Optional

logger = logging.getLogger(__name__)


class DeviceInfo(TypedDict):
    """Metadata and output for a specific device."""
    model: Optional[str]
    model_id: Optional[str]
    name: Optional[str]
    output_lines: List[str]
    components: List[str]  # Store component types for filtering


device_info: Dict[str, DeviceInfo] = {}
component_describers = {}


def register_describer(component):
    def decorator(func):
        component_describers[component] = func
        return func
    return decorator


@register_describer('switch')
def describe_switch(payload):
    yield f"State Topic:   {payload['state_topic']}"
    yield f"Command Topic: {payload['command_topic']}"
    yield f"Payload On:    {payload['payload_on']}"
    yield f"Payload Off:   {payload['payload_off']}"


@register_describer('light')
def describe_light(payload):
    yield f"State Topic:   {payload.get('state_topic', 'N/A')}"
    yield f"Command Topic: {payload.get('command_topic', 'N/A')}"

    # Show brightness support if available
    if 'brightness' in payload and payload['brightness']:
        yield f"Brightness:    Supported"
        if 'brightness_scale' in payload:
            yield f"Brightness Scale: {payload['brightness_scale']}"

    # Show color support
    if 'color_mode' in payload and payload['color_mode']:
        yield f"Color Mode:    Supported"
        yield f"Color Modes:   {', '.join(payload.get('supported_color_modes', []))}"

    # Show effect list if available
    if 'effect' in payload and payload.get('effect_list'):
        yield f"Effects:       {', '.join(payload['effect_list'])}"

    # Show on/off payloads
    yield f"Payload On:    {payload.get('payload_on', 'N/A')}"
    yield f"Payload Off:   {payload.get('payload_off', 'N/A')}"

    # Schema
    yield f"Schema:        {payload.get('schema', 'N/A')}"


@register_describer('sensor')
def describe_sensor(payload):
    yield f"State Topic:   {payload.get('state_topic', 'N/A')}"

    # Show unit of measurement if available
    if 'unit_of_measurement' in payload:
        yield f"Unit:          {payload['unit_of_measurement']}"

    # Show device class
    if 'device_class' in payload:
        yield f"Device Class:  {payload['device_class']}"

    # Show value template if available
    if 'value_template' in payload:
        yield f"Value Template: {payload['value_template']}"

    # Show state class if available
    if 'state_class' in payload:
        yield f"State Class:   {payload['state_class']}"

    # Show expire_after if available (for presence sensors)
    if 'expire_after' in payload:
        yield f"Expire After:  {payload['expire_after']} seconds"


@register_describer('device_automation')
def describe_device_automation(payload):
    # Device automations are triggers or actions
    automation_type = payload.get('automation_type', 'Unknown')
    yield f"Type:          {automation_type}"

    # Show the type of the automation
    if 'type' in payload:
        yield f"Trigger Type:  {payload['type']}"

    # Show subtype if available
    if 'subtype' in payload:
        yield f"Subtype:       {payload['subtype']}"

    # Show topic
    if 'topic' in payload:
        yield f"Topic:         {payload['topic']}"

    # Show payload
    if 'payload' in payload:
        if isinstance(payload['payload'], dict):
            yield f"Payload:       {json.dumps(payload['payload'])}"
        else:
            yield f"Payload:       {payload['payload']}"

    # Show qos if available
    if 'qos' in payload:
        yield f"QoS:           {payload['qos']}"


def on_discovery_message(client, userdata, msg):
    """Process discovery messages."""
    logger.debug(f"Discovery message received on {msg.topic}: {msg.payload}")
    try:
        payload = json.loads(msg.payload)

        # Extract entity information
        topic_parts = msg.topic.split("/")
        if len(topic_parts) == 4:
            # missing the node_id which is optional
            topic_parts.insert(2, "")
        _, component, node_id, object_id, _ = topic_parts

        if component in ['update', 'select'] or object_id in ['linkquality', 'identify']:
            return

        # Initialize device info for this node if it doesn't exist yet
        if node_id not in device_info:
            device_info[node_id] = {
                "model": None,
                "model_id": None,
                "name": None,
                "output_lines": [],
                "components": []
            }

        # Add a component type to the list if not already present
        if component not in device_info[node_id]["components"]:
            device_info[node_id]["components"].append(component)

        # Extract device information from the payload if available
        if "device" in payload:
            device_data = payload["device"]
            device_info[node_id]["model"] = device_data["model"]
            device_info[node_id]["model_id"] = device_data["model_id"]
            device_info[node_id]["name"] = device_data["name"]

        # Describe the object
        device_info[node_id]["output_lines"].append(object_id)
        device_info[node_id]["output_lines"].append("=" * len(object_id))
        if component in component_describers:
            device_info[node_id]["output_lines"].extend(component_describers[component](payload))
        else:
            device_info[node_id]["output_lines"].append(f"Don't know how to describe: {component}")
        device_info[node_id]["output_lines"].append("")

    except Exception as e:
        logger.error(f"Error processing discovery message: {e}")


@click.command("discover")
@click.option('--timeout', default=1, help='Timeout in seconds for discovery')
@click.option('--filter', help='Filter results by substring (case-insensitive)')
@click.pass_context
def discover_cmd(ctx, timeout, filter):
    """Discover devices using Home Assistant MQTT discovery."""
    client = ctx.obj

    # Set message callback
    client.on_message = on_discovery_message

    # Subscribe to Home Assistant discovery topics
    discovery_topics = [
        "homeassistant/+/+/config",   # Without Node ID
        "homeassistant/+/+/+/config"  # With Node ID
    ]

    for topic in discovery_topics:
        client.subscribe(topic)
        logger.info(f"Subscribed to {topic}")

    click.echo(f"Discovering devices for {timeout} seconds...")

    # Wait for discoveries to come in
    time.sleep(timeout)

    # Print results
    click.echo("\nDiscovery Results:")
    click.echo("=================\n")

    # Store matched device count for summary
    matched_devices = 0
    total_devices = len(device_info)

    for node_id, info in device_info.items():
        device_name = info["name"] or "Unknown"
        model = info["model"] or "Unknown model"
        model_id = info["model_id"] or "Unknown model ID"

        # Check if we need to filter this device
        if filter:
            filter_lower = filter.lower()
            # Check if the filter matches any of the device fields
            device_matches = any([
                filter_lower in node_id.lower(),
                filter_lower in device_name.lower(),
                filter_lower in model.lower(),
                filter_lower in model_id.lower(),
            ])

            # Check if the filter matches any of the component types
            component_matches = any(filter_lower in comp.lower() for comp in info["components"])

            # Check if the filter matches any of the object_ids or other content
            content_matches = False
            for line in info["output_lines"]:
                if filter_lower in line.lower():
                    content_matches = True
                    break

            # Skip this device if it doesn't match the filter
            if not (device_matches or component_matches or content_matches):
                continue

        # If we get here, the device matched the filter or no filter was specified
        matched_devices += 1

        click.echo(f"Node {node_id}:")
        click.echo(f"  Name: {device_name}")
        click.echo(f"  Model: {model}")
        click.echo(f"  Model ID: {model_id}")
        click.echo("  Objects:")

        for line in info["output_lines"]:
            click.echo(f"    {line}")
        click.echo("")

    # Print summary if a filter was applied
    if filter:
        click.echo(f"Showing {matched_devices} of {total_devices} devices matching filter: '{filter}'")
