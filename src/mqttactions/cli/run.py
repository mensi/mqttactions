import click
import importlib.util
import logging
import os
import signal
import sys
import time

from mqttactions.runtime import register_client

logger = logging.getLogger(__name__)

# Global flag for handling keyboard interrupts
running = True


def handle_signal(sig, frame):
    """Handle interrupt signals gracefully."""
    global running
    click.echo("\nStopping...")
    running = False


def load_script(script_path: str) -> bool:
    """Load a Python script file as a module.

    Args:
        script_path: Path to the Python script

    Returns:
        True if a script was loaded successfully, False otherwise
    """
    try:
        # Get the absolute path and check if the file exists
        abs_path = os.path.abspath(script_path)
        if not os.path.isfile(abs_path):
            logger.error(f"Script file not found: {abs_path}")
            return False

        # Extract module name from filename without extension
        module_name = os.path.splitext(os.path.basename(script_path))[0]

        # Create a unique module name to avoid conflicts
        unique_module_name = f"mqttactions_script_{module_name}"
        if unique_module_name in sys.modules:
            logger.error(f"Module name {unique_module_name} already in use")
            return False

        # Load the module
        logger.debug(f"Loading script: {abs_path} as {unique_module_name}")
        spec = importlib.util.spec_from_file_location(unique_module_name, abs_path)
        if spec is None or spec.loader is None:
            logger.error(f"Failed to create module spec for {abs_path}")
            return False

        module = importlib.util.module_from_spec(spec)
        sys.modules[unique_module_name] = module
        spec.loader.exec_module(module)

        logger.info(f"Successfully loaded script: {script_path}")
        return True

    except Exception as e:
        logger.error(f"Error loading script {script_path}: {e}")
        return False


@click.command("run")
@click.argument('script_paths', nargs=-1, type=click.Path(exists=True))
@click.pass_context
def run_cmd(ctx, script_paths):
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
    loaded_scripts = 0
    for script_path in script_paths:
        if load_script(script_path):
            loaded_scripts += 1

    if loaded_scripts == 0:
        click.echo("Error: No scripts were loaded successfully")
        client.disconnect()
        return 1

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

    click.echo("Automation stopped")
    return 0
