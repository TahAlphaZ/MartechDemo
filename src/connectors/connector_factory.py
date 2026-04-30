"""
Connector Factory - Dynamically instantiates connectors from registry config.
Subagents: Register new connector classes in CONNECTOR_CLASS_MAP.
"""
from __future__ import annotations

import ast
from pathlib import Path
from typing import Any, Dict, List, Tuple

from src.connectors.base_connector import (
    APIConnector,
    BaseConnector,
    ConnectorConfig,
    DatabaseConnector,
    FileConnector,
)

# ============================================================
# CONNECTOR CLASS MAP
# Subagents: Add your connector class here after implementing it.
# Format: "registry_key": ConnectorClassName
# ============================================================
CONNECTOR_CLASS_MAP: Dict[str, type] = {
    # Database connectors
    "postgres": DatabaseConnector,
    "sqlserver": DatabaseConnector,
    "mysql": DatabaseConnector,
    # File storage connectors
    "sftp": FileConnector,
    "azure_blob": FileConnector,
    # Ecommerce connectors (override with specific classes when ready)
    "shopify": APIConnector,
    "magento": APIConnector,
    "woocommerce": APIConnector,
    # Analytics connectors (override with specific classes when ready)
    "google_analytics": APIConnector,
    "adobe_analytics": APIConnector,
    "mixpanel": APIConnector,
}


def _strip_inline_comment(line: str) -> str:
    in_single = False
    in_double = False
    result: List[str] = []

    for char in line:
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif char == "#" and not in_single and not in_double:
            break
        result.append(char)

    return "".join(result).rstrip()


def _parse_scalar(value: str) -> Any:
    value = value.strip()
    if value == "":
        return ""
    if value in {"[]", "{}"}:
        return ast.literal_eval(value)
    if value.startswith(("'", '"')) and value.endswith(("'", '"')):
        return ast.literal_eval(value)
    if value.lstrip("-").isdigit():
        return int(value)
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(item.strip()) for item in inner.split(",")]
    return value


def _load_simple_yaml(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as handle:
        lines = handle.readlines()

    root: Dict[str, Any] = {}
    stack: List[Tuple[int, Any]] = [(-1, root)]

    for index, raw_line in enumerate(lines):
        line = _strip_inline_comment(raw_line)
        if not line.strip():
            continue

        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()

        while len(stack) > 1 and indent <= stack[-1][0]:
            stack.pop()

        parent = stack[-1][1]
        if stripped.startswith("- "):
            if not isinstance(parent, list):
                raise ValueError(f"Invalid YAML list structure in {config_path}: {raw_line.rstrip()}")
            parent.append(_parse_scalar(stripped[2:]))
            continue

        key, _, raw_value = stripped.partition(":")
        if not _:
            raise ValueError(f"Invalid YAML entry in {config_path}: {raw_line.rstrip()}")

        raw_value = raw_value.strip()
        if raw_value:
            parent[key] = _parse_scalar(raw_value)
            continue

        next_container: Any = {}
        for future_raw_line in lines[index + 1:]:
            future_line = _strip_inline_comment(future_raw_line)
            if not future_line.strip():
                continue
            future_indent = len(future_line) - len(future_line.lstrip(" "))
            if future_indent <= indent:
                break
            next_container = [] if future_line.strip().startswith("- ") else {}
            break

        parent[key] = next_container
        stack.append((indent, next_container))

    return root


def load_registry(config_path: str = None) -> Dict[str, Any]:
    """Load connector registry YAML config."""
    if config_path is None:
        config_path = str(
            Path(__file__).parent.parent.parent / "config" / "connectors" / "connector_registry.yaml"
        )

    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:
        return _load_simple_yaml(config_path)

    with open(config_path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def get_active_connectors(registry: Dict[str, Any]) -> Dict[str, ConnectorConfig]:
    """
    Parse registry and return only active connector configs.
    This is what the pipeline uses to determine what to ingest.
    """
    active = {}

    # Database connector
    db_section = registry.get("databases", {})
    active_db = db_section.get("active", "postgres")
    db_config = db_section.get("connectors", {}).get(active_db, {})
    active[f"db_{active_db}"] = ConnectorConfig(
        name=active_db,
        connector_type="database",
        keyvault_secret=db_config.get("keyvault_secret", ""),
        landing_path=f"raw/database/{active_db}",
        extra=db_config,
    )

    # File storage connector
    fs_section = registry.get("file_storage", {})
    active_fs = fs_section.get("active", "sftp")
    fs_config = fs_section.get("connectors", {}).get(active_fs, {})
    active[f"file_{active_fs}"] = ConnectorConfig(
        name=active_fs,
        connector_type="file",
        keyvault_secret=fs_config.get("keyvault_secret", ""),
        landing_path=fs_config.get("landing_path", "raw/files"),
        extra=fs_config,
    )

    # Ecommerce connectors
    ecom_section = registry.get("ecommerce", {})
    for ecom_name in ecom_section.get("active", []):
        ecom_config = ecom_section.get("connectors", {}).get(ecom_name, {})
        active[f"ecom_{ecom_name}"] = ConnectorConfig(
            name=ecom_name,
            connector_type="api",
            keyvault_secret=ecom_config.get("keyvault_secret", ""),
            landing_path=ecom_config.get("landing_path", f"raw/ecommerce/{ecom_name}"),
            extra=ecom_config,
        )

    # Analytics connectors
    analytics_section = registry.get("analytics", {})
    for analytics_name in analytics_section.get("active", []):
        analytics_config = analytics_section.get("connectors", {}).get(analytics_name, {})
        active[f"analytics_{analytics_name}"] = ConnectorConfig(
            name=analytics_name,
            connector_type="analytics",
            keyvault_secret=analytics_config.get("keyvault_secret", ""),
            landing_path=analytics_config.get("landing_path", f"raw/analytics/{analytics_name}"),
            extra=analytics_config,
        )

    return active


def create_connector(config: ConnectorConfig, secret_value: str) -> BaseConnector:
    """
    Factory method to create connector instance from config.
    Secret is resolved from Key Vault at runtime.
    """
    connector_class = CONNECTOR_CLASS_MAP.get(config.name)
    if connector_class is None:
        raise ValueError(
            f"No connector class registered for '{config.name}'. "
            f"Register it in CONNECTOR_CLASS_MAP in connector_factory.py"
        )

    if issubclass(connector_class, DatabaseConnector):
        return connector_class(config, connection_string=secret_value)
    if issubclass(connector_class, FileConnector):
        return connector_class(config, credentials=secret_value)
    if issubclass(connector_class, APIConnector):
        return connector_class(config, api_key=secret_value)

    raise ValueError(f"Unknown connector base class for '{config.name}'")
