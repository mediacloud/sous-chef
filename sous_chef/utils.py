"""
General utility functions for sous-chef.
"""
import hashlib
import logging
import os
import re
import time
import unicodedata

# Try to import Prefect logging utilities
try:
    from prefect.logging import get_run_logger
    from prefect.exceptions import MissingContextError
    PREFECT_LOGGING_AVAILABLE = True
except ImportError:
    PREFECT_LOGGING_AVAILABLE = False
    get_run_logger = None
    MissingContextError = None


def get_logger() -> logging.Logger:
    """
    Get a logger that works with or without Prefect context.
    
    Falls back to standard Python logging if Prefect context is unavailable.
    This allows tasks and flows to work when called directly (e.g., via .fn())
    without requiring a Prefect server or active flow run.
    
    Returns:
        A logger instance (either Prefect's run logger or standard Python logger)
    """
    if not PREFECT_LOGGING_AVAILABLE:
        return logging.getLogger("sous_chef")
    
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger("sous_chef")


def is_test_mode() -> bool:
    """
    Check if we're running in test mode.
    
    Test mode is enabled when:
    - SOUS_CHEF_TEST_MODE environment variable is set to a truthy value
    - SOUS_CHEF_SKIP_B2 environment variable is set to a truthy value
    
    This is automatically set when using run_flow.py with the --test flag.
    
    Returns:
        True if test mode is enabled, False otherwise
    """
    return (
        os.getenv("SOUS_CHEF_TEST_MODE", "").lower() in ("true", "1", "yes") or
        os.getenv("SOUS_CHEF_SKIP_B2", "").lower() in ("true", "1", "yes")
    )


def create_url_safe_slug(text: str, max_length: int = 16) -> str:
    """
    Create a URL-safe slug from a string, with a maximum length.
    
    Converts text to lowercase, removes/replaces special characters,
    and truncates to max_length while preserving readability.
    
    Args:
        text: Input string to convert to slug
        max_length: Maximum length of the slug (default: 16)
        
    Returns:
        URL-safe slug string
        
    Examples:
        >>> create_url_safe_slug("Hello World!")
        'hello-world'
        >>> create_url_safe_slug("This is a very long query string", max_length=16)
        'this-is-a-very'
        >>> create_url_safe_slug("Query with émojis 🎉 and symbols!")
        'query-with-emojis'
    """
    if not text:
        return ""
    
    # Convert to lowercase
    slug = text.lower()
    
    # Remove unicode accents/diacritics (é -> e, ñ -> n, etc.)
    slug = unicodedata.normalize('NFKD', slug)
    slug = ''.join(c for c in slug if not unicodedata.combining(c))
    
    # Replace spaces and underscores with hyphens
    slug = re.sub(r'[\s_]+', '-', slug)
    
    # Remove all non-alphanumeric characters except hyphens
    slug = re.sub(r'[^a-z0-9-]', '', slug)
    
    # Replace multiple consecutive hyphens with a single hyphen
    slug = re.sub(r'-+', '-', slug)
    
    # Remove leading and trailing hyphens
    slug = slug.strip('-')
    
    # Truncate to max_length, but try to break at a hyphen if possible
    if len(slug) > max_length:
        # Try to find a hyphen near the max_length to break at
        truncated = slug[:max_length]
        last_hyphen = truncated.rfind('-')
        if last_hyphen > max_length * 0.5:  # Only use hyphen break if it's not too early
            slug = truncated[:last_hyphen]
        else:
            slug = truncated
    
    # Remove any trailing hyphen after truncation
    slug = slug.rstrip('-')
    
    # Add a short time hash at the end to keep separate runs separate
    timestamp = str(time.time())
    time_hash = hashlib.md5(timestamp.encode()).hexdigest()[:6]
    slug = f"{slug}-{time_hash}" if slug else time_hash
    
    return slug or "slug"  # Return a default if empty