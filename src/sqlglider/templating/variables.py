"""Variable loading utilities for SQL templating.

This module provides functions for loading template variables from
multiple sources with a defined priority order:
1. CLI arguments (highest priority)
2. Variables file (JSON/YAML/TOML)
3. Config file inline variables
4. Environment variables (lowest priority)
"""

import json
import os
import tomllib
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.console import Console

console = Console(stderr=True)


def load_variables_file(path: Path) -> Dict[str, Any]:
    """Load variables from a JSON, YAML, or TOML file.

    Args:
        path: Path to the variables file. Must have .json, .yaml, .yml, or .toml extension.

    Returns:
        A dictionary of variables loaded from the file.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file format is not supported or cannot be parsed.
    """
    if not path.exists():
        raise FileNotFoundError(f"Variables file not found: {path}")

    suffix = path.suffix.lower()

    if suffix == ".json":
        return _load_json_file(path)
    elif suffix in (".yaml", ".yml"):
        return _load_yaml_file(path)
    elif suffix == ".toml":
        return _load_toml_file(path)
    else:
        raise ValueError(
            f"Unsupported variables file format: {suffix}. "
            "Use .json, .yaml, .yml, or .toml"
        )


def _load_json_file(path: Path) -> Dict[str, Any]:
    """Load variables from a JSON file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError(
                f"Variables file {path} must contain a JSON object, "
                f"got {type(data).__name__}"
            )

        return data

    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {path}: {e}") from e


def _load_yaml_file(path: Path) -> Dict[str, Any]:
    """Load variables from a YAML file.

    Requires PyYAML to be installed.
    """
    try:
        import yaml
    except ImportError:
        raise ValueError(
            f"Cannot load YAML file {path}: PyYAML is not installed. "
            "Install it with: uv add pyyaml"
        )

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if data is None:
            return {}

        if not isinstance(data, dict):
            raise ValueError(
                f"Variables file {path} must contain a YAML mapping, "
                f"got {type(data).__name__}"
            )

        return data

    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in {path}: {e}") from e


def _load_toml_file(path: Path) -> Dict[str, Any]:
    """Load variables from a TOML file.

    Uses Python 3.11+ built-in tomllib.
    """
    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)

        if not isinstance(data, dict):
            raise ValueError(
                f"Variables file {path} must contain a TOML table, "
                f"got {type(data).__name__}"
            )

        return data

    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"Invalid TOML in {path}: {e}") from e


def parse_cli_variables(var_args: Optional[List[str]]) -> Dict[str, Any]:
    """Parse CLI variable arguments in key=value format.

    Supports basic type inference:
    - Integers: 123
    - Floats: 12.34
    - Booleans: true, false (case-insensitive)
    - Strings: everything else

    Args:
        var_args: List of variable strings in "key=value" format.

    Returns:
        A dictionary of parsed variables.

    Raises:
        ValueError: If a variable string is not in key=value format.
    """
    if not var_args:
        return {}

    variables: Dict[str, Any] = {}

    for var_str in var_args:
        if "=" not in var_str:
            raise ValueError(
                f"Invalid variable format: '{var_str}'. Expected 'key=value'"
            )

        key, value = var_str.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key:
            raise ValueError(f"Empty variable name in: '{var_str}'")

        variables[key] = _infer_type(value)

    return variables


def _infer_type(value: str) -> Any:
    """Infer the type of a string value.

    Args:
        value: The string value to parse.

    Returns:
        The value converted to the inferred type.
    """
    # Check for boolean
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False

    # Check for integer
    try:
        return int(value)
    except ValueError:
        pass

    # Check for float
    try:
        return float(value)
    except ValueError:
        pass

    # Default to string
    return value


def load_env_variables(prefix: str = "SQLGLIDER_VAR_") -> Dict[str, Any]:
    """Load variables from environment variables.

    Environment variables with the specified prefix are loaded and the
    prefix is stripped from the key. For example, SQLGLIDER_VAR_SCHEMA
    becomes the variable "schema" (lowercased).

    Args:
        prefix: The prefix to look for in environment variable names.

    Returns:
        A dictionary of variables from environment variables.
    """
    variables: Dict[str, Any] = {}

    for key, value in os.environ.items():
        if key.startswith(prefix):
            # Remove prefix and lowercase the key
            var_name = key[len(prefix) :].lower()
            if var_name:
                variables[var_name] = _infer_type(value)

    return variables


def merge_variables(*sources: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge variables from multiple sources with priority.

    Later sources in the argument list have higher priority and will
    override values from earlier sources.

    Args:
        *sources: Variable dictionaries to merge, from lowest to highest priority.
                 None values are skipped.

    Returns:
        A merged dictionary of variables.

    Example:
        >>> env_vars = {"schema": "default", "table": "users"}
        >>> config_vars = {"schema": "config_schema"}
        >>> cli_vars = {"schema": "cli_schema", "column": "id"}
        >>> merged = merge_variables(env_vars, config_vars, cli_vars)
        >>> print(merged)
        {"schema": "cli_schema", "table": "users", "column": "id"}
    """
    result: Dict[str, Any] = {}

    for source in sources:
        if source is not None:
            result.update(source)

    return result


def load_all_variables(
    cli_vars: Optional[List[str]] = None,
    vars_file: Optional[Path] = None,
    config_vars: Optional[Dict[str, Any]] = None,
    use_env: bool = True,
) -> Dict[str, Any]:
    """Load and merge variables from all sources.

    Priority order (highest to lowest):
    1. CLI arguments
    2. Variables file
    3. Config file inline variables
    4. Environment variables

    Args:
        cli_vars: List of CLI variable strings in "key=value" format.
        vars_file: Path to a variables file (JSON, YAML, or TOML).
        config_vars: Variables from the configuration file.
        use_env: Whether to load environment variables.

    Returns:
        A merged dictionary of variables from all sources.
    """
    sources: List[Optional[Dict[str, Any]]] = []

    # Load in priority order (lowest first)
    if use_env:
        sources.append(load_env_variables())

    if config_vars:
        sources.append(config_vars)

    if vars_file:
        try:
            sources.append(load_variables_file(vars_file))
        except (FileNotFoundError, ValueError) as e:
            console.print(f"[yellow]Warning:[/yellow] {e}")

    if cli_vars:
        try:
            sources.append(parse_cli_variables(cli_vars))
        except ValueError as e:
            console.print(f"[yellow]Warning:[/yellow] {e}")

    return merge_variables(*sources)
