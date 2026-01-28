"""
Example flows for sous-chef demo.

These flows demonstrate the sous-chef architecture with simple examples.
"""

# Import flows to register them
from .keywords_demo_flow import keywords_demo_flow
from .entities_demo_flow import entities_demo_flow

__all__ = [
    "keywords_demo_flow",
    "entities_demo_flow",
]
