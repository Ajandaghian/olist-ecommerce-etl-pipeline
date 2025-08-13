from yaml import safe_load
import pathlib


DIRECTORY = pathlib.Path(__file__).parent
CONFIG_FILE = DIRECTORY / "config.yaml"


def load_config():
    """Load configuration from YAML file."""
    with open(CONFIG_FILE, 'r') as file:
        config = safe_load(file)
    return config


config = load_config()
