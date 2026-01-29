"""Configuration management for SQL Glider.

Loads configuration from sqlglider.toml in the current working directory.
"""

import tomllib
from pathlib import Path
from typing import Any, Dict, Optional

from pydantic import BaseModel
from rich.console import Console

console = Console(stderr=True)


class TemplatingConfig(BaseModel):
    """Configuration for the templating system.

    All fields are optional.
    """

    variables_file: Optional[str] = None
    variables: Optional[Dict[str, Any]] = None


class DatabricksCatalogConfig(BaseModel):
    """Configuration for Databricks catalog provider.

    All fields are optional - they can also be set via environment variables.
    The SDK supports unified authentication with multiple methods.
    """

    warehouse_id: Optional[str] = None
    profile: Optional[str] = None  # Databricks CLI profile from ~/.databrickscfg
    host: Optional[str] = None
    token: Optional[str] = None  # Legacy PAT, prefer OAuth or profile


class CatalogConfig(BaseModel):
    """Configuration for catalog providers.

    Contains provider-specific configuration under sub-keys.
    """

    databricks: Optional[DatabricksCatalogConfig] = None


class ConfigSettings(BaseModel):
    """Configuration settings for SQL Glider.

    All fields are optional. None values indicate the setting was not
    specified in the config file.
    """

    dialect: Optional[str] = None
    level: Optional[str] = None
    output_format: Optional[str] = None
    templater: Optional[str] = None
    templating: Optional[TemplatingConfig] = None
    catalog_type: Optional[str] = None
    ddl_folder: Optional[str] = None
    catalog: Optional[CatalogConfig] = None
    no_star: Optional[bool] = None
    resolve_schema: Optional[bool] = None
    dump_schema: Optional[str] = None
    dump_schema_format: Optional[str] = None
    strict_schema: Optional[bool] = None


def find_config_file(start_path: Optional[Path] = None) -> Optional[Path]:
    """Find sqlglider.toml in the current working directory.

    Args:
        start_path: Starting directory to search for config file.
                   Defaults to current working directory.

    Returns:
        Path to config file if found, None otherwise.
    """
    if start_path is None:
        start_path = Path.cwd()

    config_path = start_path / "sqlglider.toml"

    if config_path.exists() and config_path.is_file():
        return config_path

    return None


def load_config(config_path: Optional[Path] = None) -> ConfigSettings:
    """Load configuration from sqlglider.toml.

    Priority order:
    1. Explicit config_path parameter
    2. sqlglider.toml in current working directory
    3. Empty ConfigSettings (all None)

    Args:
        config_path: Optional explicit path to config file.
                    If not provided, searches current working directory.

    Returns:
        ConfigSettings with values from TOML file or None for unset fields.
        Always returns a valid ConfigSettings object, even on errors.

    Error Handling:
        - Missing file: Returns empty ConfigSettings (silent)
        - Malformed TOML: Warns user and returns empty ConfigSettings
        - Invalid values: Warns user and sets affected fields to None
        - Unknown keys: Ignored (forward compatibility)
    """
    # Find config file
    if config_path is None:
        config_path = find_config_file()

    # No config file found - return empty settings
    if config_path is None:
        return ConfigSettings()

    try:
        # Read and parse TOML file
        with open(config_path, "rb") as f:
            toml_data = tomllib.load(f)

        # Extract sqlglider section
        sqlglider_config = toml_data.get("sqlglider", {})

        # Validate and create ConfigSettings
        # Pydantic will validate types and ignore unknown fields
        try:
            return ConfigSettings(**sqlglider_config)
        except Exception as e:
            console.print(
                f"[yellow]Warning:[/yellow] Invalid configuration in {config_path}: {e}",
            )
            console.print("[yellow]Using default settings[/yellow]")
            return ConfigSettings()

    except tomllib.TOMLDecodeError as e:
        console.print(
            f"[yellow]Warning:[/yellow] Failed to parse {config_path}: {e}",
        )
        console.print("[yellow]Using default settings[/yellow]")
        return ConfigSettings()

    except (OSError, IOError) as e:
        console.print(
            f"[yellow]Warning:[/yellow] Could not read {config_path}: {e}",
        )
        console.print("[yellow]Using default settings[/yellow]")
        return ConfigSettings()

    except Exception as e:
        # Catch-all for unexpected errors
        console.print(
            f"[yellow]Warning:[/yellow] Unexpected error loading config: {e}",
        )
        console.print("[yellow]Using default settings[/yellow]")
        return ConfigSettings()
