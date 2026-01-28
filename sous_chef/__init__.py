"""
Sous-Chef v3.0.0-alpha - Data pipeline tool with Python flow definitions.

This package provides a flow-based architecture for building data pipelines.
Flows are defined as Python functions with Pydantic parameter models.
"""

from .flow import (
    register_flow,
    get_flow,
    list_flows,
    get_flow_schema,
)

__all__ = [
    "register_flow",
    "get_flow",
    "list_flows",
    "get_flow_schema",
]

__version__ = "3.0.0-alpha"
