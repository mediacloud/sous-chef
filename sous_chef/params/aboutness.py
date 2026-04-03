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
    generic = "generic"
    custom = "custom"


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
        title="Aboutness context (used when kind is Custom)",
        description=(
            "Used only when target kind is \"Custom\". Provide your own background "
            "description to help the LLM judge aboutness (e.g., a list of LGAs, "
            "a topic definition, or key details about the target). Ignored for "
            "other target kinds."
        ),
    )

    about_target_kind: AboutnessTargetKind = Field(
        default=AboutnessTargetKind.generic,
        title="Aboutness target kind",
        description=(
            "Type of the aboutness target. A preset context is generated for "
            "Geography, Person, Organization, Topic, or Generic. Choose \"Custom\" "
            "to supply your own context in the Aboutness context field."
        ),
    )

    upload_prefiltered_rows: bool = Field(
        default=False,
        title="Upload pre-threshold aboutness rows",
        description=(
            "If enabled, the aboutness scoring task uploads a CSV of every row after "
            "LLM scoring but before the aboutness threshold filter (all scored articles)."
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
            "The subject is a geographic place (a region and its residents and authorities), "
            "not a person or company. Most articles that mention this place are acceptable "
            "unless the mention is clearly incidental or about something else. "
            "Treat mentions as NOT about the place when the name only appears briefly in a "
            "long list of locations, in a single short dateline or weather report, or as part "
            "of a quote with no further discussion of conditions, events, or decisions "
            f"affecting {target} or people living there. Also treat it as NOT about the "
            "intended place when the name clearly refers to a different entity (for example, "
            "a company, product, or person that happens to share the same name)."
        )
    if kind == AboutnessTargetKind.person:
        return (
            "The subject is a specific person, not any other person or organization with a "
            "similar name. Most articles that substantially discuss this person should be "
            "treated as about them. Treat mentions as NOT about this person when their name "
            "only appears once or twice in passing (for example, in a list of many people, "
            "a brief quote, or a background reference) and there is no sustained discussion "
            "of their actions, decisions, role, reputation, or the consequences of what they "
            "do. Also treat it as NOT about the intended person when it is clearly a different "
            "individual with the same or similar name, or a different kind of entity entierly"
        )
    if kind == AboutnessTargetKind.organization:
        return (
            "The subject is a specific organization or company, not a geographic region or "
            "generic concept. Most articles that meaningfully discuss this organization's "
            "activities are acceptable. Treat mentions as NOT about this organization when "
            "the name only appears in a long list of partners, sponsors, or examples, or in "
            "boilerplate (such as a stock index list or directory) without any real discussion "
            "of its decisions, policies, finances, internal issues, or projects. Also treat it "
            "as NOT about the intended organization when the name clearly refers to a different "
            "entity that happens to share the same or a similar name, or a different kind of "
            "entity entierly"
        )
    if kind == AboutnessTargetKind.topic:
        return (
            "The subject is a policy area or topic, not a single event or organization. "
            "Most articles that substantially discuss this topic should be treated as "
            "about it. Treat mentions as NOT about the topic when it appears only as a "
            "buzzword or side note (for example, in a list of many unrelated issues, a "
            "brief slogan, or a passing reference) and there is no real description of "
            "problems, causes, impacts, debates, interventions, or policies centered on "
            "this topic. If the main focus of the article lies elsewhere and the topic is "
            "only touched on superficially, it should be treated as not about the topic."
        )

    if kind == AboutnessTargetKind.generic:
        return (
            "The subject is the main thing we care about in this analysis. Most articles "
            "that discuss this subject in a meaningful way should be treated as about it. "
            "Treat mentions as NOT about the subject when it is only mentioned briefly or "
            "incidentally (for example, in a long list, a single short reference, or "
            "boilerplate text) and there is no sustained discussion of the subject's "
            "situation, actions, impacts, or related events. If the connection to the "
            "subject is clearly minor or the name refers to something else entirely, the "
            "article should be treated as not about the subject."
        )

    # custom: caller should use about_context; fallback below if build_default_about_context
    # is ever called with custom.

    # Fallback: very generic guidance if a new kind is introduced without
    # updating this function.
    return (
        "The subject is the main thing we care about in this analysis. Stories "
        "should be treated as about this subject when they discuss it in a "
        "meaningful way, rather than only mentioning it briefly or incidentally."
    )

