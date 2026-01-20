"""
Task library for sous-chef flows.

Tasks are Prefect-decorated functions that can be used within flows.
"""

from .discovery_tasks import query_online_news
from .keyword_tasks import extract_keywords
from .aggregator_tasks import top_n_unique_values
from .export_tasks import csv_to_b2

__all__ = [
    "query_online_news",
    "extract_keywords",
    "top_n_unique_values",
    "csv_to_b2",
]
