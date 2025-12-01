import importlib.util
import logging
import os
import sys
from typing import List

logger = logging.getLogger(__name__)


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


def load_scripts(script_paths: List[str]) -> int:
    """Load multiple Python scripts.

    Args:
        script_paths: List of paths to Python scripts

    Returns:
        Number of successfully loaded scripts
    """
    loaded_count = 0
    for script_path in script_paths:
        if load_script(script_path):
            loaded_count += 1
    return loaded_count
