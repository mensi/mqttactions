import click
import logging
import signal
import time

from mqttactions.loader import load_scripts
from mqttactions.runtime import register_client

logger = logging.getLogger(__name__)

# Global flag for handling keyboard interrupts
running = True


def handle_signal(sig, frame):
    """Handle interrupt signals gracefully."""
    global running
    click.echo("\nStopping...")
    running = False


@click.command("run")
@click.argument('script_paths', nargs=-1, type=click.Path(exists=True))
@click.option('--web-port', type=int, default=None, help='Run the web interface on this port')
@click.pass_context
def run_cmd(ctx, script_paths, web_port):
    """Run automation scripts that respond to MQTT messages.

    SCRIPT_PATHS: One or more Python script files to load and execute.

    Example script:

    from mqttactions import on, publish

    @on("some-switch/action", payload="press_1")
    def turn_on_light():
        publish("some-light/set", {"state": "ON"})
    """
    global running

    if not script_paths:
        click.echo("Error: At least one script file must be provided")
        return 1

    # Get MQTT client from context
    client = ctx.obj

    # Set up the global MQTT client in the mqttactions module
    register_client(client)

    # Load all script files
    loaded_scripts = load_scripts(script_paths)

    if loaded_scripts == 0:
        click.echo("Error: No scripts were loaded successfully")
        client.disconnect()
        return 1

    # Run the web interface if requested
    def shutdown_webserver():
        pass
    if web_port:
        from mqttactions.web.main import run
        shutdown_webserver = run(web_port)

    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    click.echo(f"Running with {loaded_scripts} script(s). Press Ctrl+C to stop.")

    # Keep running until interrupted
    while running:
        try:
            # This loop just keeps the main thread alive
            # The real work happens in the MQTT callback thread
            time.sleep(0.1)
        except (KeyboardInterrupt, SystemExit):
            running = False

    if web_port:
        click.echo("Stopping web server...")
        shutdown_webserver()
    click.echo("Automation stopped")
    return 0
