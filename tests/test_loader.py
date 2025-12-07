import os
import pytest
from mqttactions.loader import load_scripts

def test_load_scripts_directory(tmp_path):
    """Test loading scripts from a directory."""
    # Create a temporary directory structure
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    
    # Create dummy scripts
    (scripts_dir / "script1.py").write_text("print('Loaded script1')")
    (scripts_dir / "script2.py").write_text("print('Loaded script2')")
    (scripts_dir / "not_a_script.txt").write_text("This should be ignored")
    
    # Create a subdirectory (should be ignored)
    subdir = scripts_dir / "subdir"
    subdir.mkdir()
    (subdir / "script3.py").write_text("print('Loaded script3')")
    
    # Load scripts from the directory
    loaded_count = load_scripts([str(scripts_dir)])
    
    # Verify that only the 2 scripts in the top level were loaded
    assert loaded_count == 2
