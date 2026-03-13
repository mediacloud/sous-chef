from __future__ import annotations

"""
Reusable parameters for LLM aboutness tasks.

Intended to be mixed into flow parameter models so multiple flows can
share a consistent way to configure the target subject and optional
context for aboutness judgments.
"""

from typing import ClassVar, Optional

from pydantic import BaseModel, Field


class AboutnessParams(BaseModel):
    """
    Parameters for configuring LLM aboutness judgments.

    This model is generic: it can be used for geography, topics,
    organizations, people, etc.
    """

    _component_hint: ClassVar[str] = "AboutnessParams"

    about_target: str = Field(
        title="Aboutness target",
        description=(
            "Subject the LLM should judge articles against "
            "(e.g., a state name, health topic, organization, or person)."
        ),
    )

    about_context: Optional[str] = Field(
        default=None,
        title="Aboutness context (optional)",
        description=(
            "Optional background description to help the LLM judge aboutness. "
            "For example: a list of LGAs for a state, a topic definition, or "
            "key details about the target."
        ),
    )

