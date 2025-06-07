import click
import logging
import traceback
import paho.mqtt.client as mqtt

from mqttactions.cli.discover import discover_cmd
from mqttactions.cli.run import run_cmd


logger = logging.getLogger(__name__)


@click.group()
@click.option('--host', default='localhost', help='MQTT broker hostname')
@click.option('--port', default=1883, type=int, help='MQTT broker port')
@click.option('--username', help='MQTT username')
@click.option('--password', help='MQTT password')
@click.pass_context
def cli(ctx: click.Context, host, port, username, password):
    """MQTT Actions CLI tool for controlling smart home devices."""
    # Connect to MQTT broker
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if username is not None:
        client.username_pw_set(username, password)

    try:
        logger.info(f"Connecting to MQTT broker at {host}:{port}")
        client.connect(host, port)
        client.loop_start()
    except Exception as e:
        click.echo(click.style(f"Failed to connect to MQTT broker: {e}", fg="red"), err=True)
        ctx.exit(1)

    ctx.obj = client

    # Register a callback to close the client when context exits
    @ctx.call_on_close
    def cleanup():
        try:
            client.disconnect()
            client.loop_stop()
            logger.info("Disconnected from MQTT broker")
        except Exception as e:
            logger.warning(f"Error disconnecting MQTT client: {e}")


# Register subcommands
cli.add_command(discover_cmd)
cli.add_command(run_cmd)


def main():
    logging.basicConfig(level=logging.INFO)
    try:
        cli()
    except Exception as e:
        click.echo(click.style('An unexpected error occurred during execution:', fg='red'), err=True)
        for line in traceback.format_exception(e):
            click.echo('  ' + line, err=True)
        return 1
