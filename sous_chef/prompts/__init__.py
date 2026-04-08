"""
Packaged LLM prompt templates.

Templates use str.format placeholders matching the corresponding Pydantic input models.
Version files (e.g. aboutness/v1.txt) can be bumped or forked without changing task code.
"""

from __future__ import annotations

from functools import lru_cache
from importlib.resources import files


@lru_cache(maxsize=64)
def load_prompt(*parts: str) -> str:
    """
    Load a UTF-8 prompt file from this package (works from source and installed wheels).

    Args:
        *parts: Path segments under ``sous_chef.prompts``, e.g. ``"aboutness", "v1.txt"``.
    """
    root = files("sous_chef.prompts")
    return root.joinpath(*parts).read_text(encoding="utf-8").strip()
