# Author: Sa-tish Chauhan

"""Configuration management utilities."""

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional


def load_config_file(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from JSON file.
    
    Args:
        config_path: Path to config file. If None, looks for 'config.json' in current directory.
    
    Returns:
        Dictionary with configuration values, or empty dict if file not found.
    """
    if config_path is None:
        config_path = "config.json"
    
    config_file = Path(config_path)
    if not config_file.exists():
        return {}
    
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        raise ValueError(f"Failed to load config file {config_path}: {e}")


def save_config_file(config: Dict[str, Any], config_path: Optional[str] = None):
    """
    Save configuration to JSON file.
    
    Args:
        config: Configuration dictionary to save
        config_path: Path to config file. If None, uses 'config.json' in current directory.
    """
    if config_path is None:
        config_path = "config.json"
    
    config_file = Path(config_path)
    config_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(config_file, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def get_input(prompt: str, default: Optional[str] = None, password: bool = False) -> str:
    """
    Get user input with optional default value.
    
    Args:
        prompt: Prompt text
        default: Default value to show
        password: If True, hide input (for passwords)
    
    Returns:
        User input or default value
    """
    if default:
        prompt_text = f"{prompt} [{default}]: "
    else:
        prompt_text = f"{prompt}: "
    
    if password:
        import getpass
        value = getpass.getpass(prompt_text)
    else:
        value = input(prompt_text).strip()
    
    return value if value else (default or "")


def get_yes_no(prompt: str, default: bool = False) -> bool:
    """
    Get yes/no input from user.
    
    Args:
        prompt: Prompt text
        default: Default value
    
    Returns:
        True for yes, False for no
    """
    default_text = "Y/n" if default else "y/N"
    response = input(f"{prompt} [{default_text}]: ").strip().lower()
    
    if not response:
        return default
    
    return response in ("y", "yes", "1", "true")


def merge_configs(*configs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge multiple configuration dictionaries.
    Later configs override earlier ones.
    
    Args:
        *configs: Configuration dictionaries to merge
    
    Returns:
        Merged configuration dictionary
    """
    result = {}
    for config in configs:
        if config:
            result.update(config)
    return result


def get_config_value(
    cli_value: Optional[Any],
    config_value: Optional[Any],
    env_key: Optional[str] = None,
    default_value: Optional[Any] = None,
    prompt: Optional[str] = None,
    password: bool = False,
    interactive: bool = False,
) -> Any:
    """
    Get configuration value with priority: CLI > Config file > Env var > Interactive > Default.
    
    Args:
        cli_value: Value from command line argument
        config_value: Value from config file
        env_key: Environment variable key (optional)
        default_value: Default value
        prompt: Prompt text for interactive input (optional)
        password: If True, hide input for passwords
        interactive: If True, prompt user if value is missing
    
    Returns:
        Configuration value
    """
    # Priority 1: CLI argument
    if cli_value is not None:
        return cli_value
    
    # Priority 2: Config file
    if config_value is not None:
        return config_value
    
    # Priority 3: Environment variable
    if env_key:
        env_value = os.environ.get(env_key)
        if env_value:
            return env_value
    
    # Priority 4: Interactive prompt
    if interactive and prompt:
        value = get_input(prompt, str(default_value) if default_value is not None else None, password=password)
        if value:
            return value
    
    # Priority 5: Default
    return default_value

