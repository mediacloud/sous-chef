from __future__ import annotations

"""
Reusable parameters for LLM aboutness tasks.

Intended to be mixed into flow parameter models so multiple flows can
share a consistent way to configure the target subject and optional
context for aboutness judgments.
"""

from typing import ClassVar, Optional
from enum import Enum

from pydantic import BaseModel, Field


class AboutnessTargetKind(str, Enum):
    """High-level type of the aboutness target, used for context presets."""

    geography = "geography"
    person = "person"
    organization = "organization"
    topic = "topic"
    other = "other"


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

    about_target_kind: Optional[AboutnessTargetKind] = Field(
        default=None,
        title="Aboutness target kind (optional)",
        description=(
            "High-level type of the aboutness target. If provided and no custom "
            "about_context is set, a default context description may be generated "
            "for the LLM (e.g., geography, person, organization, or topic)."
        ),
    )


def build_default_about_context(kind: AboutnessTargetKind, target: str) -> str:
    """
    Build a generic default aboutness context string based on the target kind.

    This is intentionally high-level and non-domain-specific, so that flows can
    use it as a sensible default while still allowing users to override
    about_context for more specific behavior.
    """

    if kind == AboutnessTargetKind.geography:
        return (
            "The subject is a geographic place (a region and its residents and "
            "authorities), not a person or company. Stories should be treated as "
            f"about this place when they discuss events, conditions, or actions "
            f"that meaningfully affect {target} or people living there."
        )
    if kind == AboutnessTargetKind.person:
        return (
            "The subject is a specific person, not any other person or organization "
            "with a similar name. Stories should be treated as about this person "
            "when they focus on their actions, statements, decisions, role, "
            "reputation, or the direct consequences of what they do."
        )
    if kind == AboutnessTargetKind.organization:
        return (
            "The subject is an organization or company, not a geographic region or "
            "generic term. Stories should be treated as about this organization when "
            "they focus on its policies, programs, finances, internal issues, "
            "projects, or major decisions, not just a brief mention in a list."
        )
    if kind == AboutnessTargetKind.topic:
        return (
            "The subject is a policy area or topic, not a single event or "
            "organization. Stories should be treated as about this topic when they "
            "substantially discuss problems, causes, impacts, debates, interventions, "
            "or policies that are clearly centered on the topic."
        )

    if kind == AboutnessTargetKind.other:
        return (
            "The subject is the main thing we care about in this analysis. Stories "
            "should be treated as about this subject when they discuss it in a "
            "meaningful way, rather than only mentioning it briefly or incidentally."
        )

    # Fallback: very generic guidance if a new kind is introduced without
    # updating this function.
    return (
        "The subject is the main thing we care about in this analysis. Stories "
        "should be treated as about this subject when they discuss it in a "
        "meaningful way, rather than only mentioning it briefly or incidentally."
    )

