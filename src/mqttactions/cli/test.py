import click
import signal
import time
import logging

from mqttactions.loader import load_scripts
from mqttactions.runtime import register_client
from mqttactions.inmemory_client import InMemoryMqttClient

logger = logging.getLogger(__name__)
running = True


def handle_signal(sig, frame):
    """Handle interrupt signals gracefully."""
    global running
    click.echo("\nStopping...")
    running = False


@click.command("test")
@click.argument('script_paths', nargs=-1, type=click.Path(exists=True))
@click.option('--port', type=int, default=8000, help='Web interface port (default: 8000)')
@click.pass_context
def test_cmd(ctx, script_paths, port):
    """Run scripts in simulation mode without a real MQTT broker.
    
    Starts a web interface where you can inject messages and observe behavior.
    """
    global running

    if not script_paths:
        click.echo("Error: At least one script file must be provided")
        return 1

    # Create and register in-memory client
    client = InMemoryMqttClient()
    register_client(client)
    
    # Store client in context for web app to access
    ctx.obj = client

    # Load scripts
    loaded_count = load_scripts(script_paths)
    if loaded_count == 0:
        click.echo("Error: No scripts were loaded successfully")
        return 1

    # Start web server
    from mqttactions.web.main import run
    shutdown_webserver = run(port)
    
    # Connect the client (simulated)
    client.connect("localhost")

    # Setup signal handlers
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    click.echo(f"Running in TEST mode with {loaded_count} script(s).")
    click.echo(f"Open http://localhost:{port} to access the simulation dashboard.")
    click.echo("Press Ctrl+C to stop.")

    while running:
        try:
            time.sleep(0.1)
        except (KeyboardInterrupt, SystemExit):
            running = False

    click.echo("Stopping web server...")
    shutdown_webserver()
    client.disconnect()
    click.echo("Test session stopped")
    return 0
