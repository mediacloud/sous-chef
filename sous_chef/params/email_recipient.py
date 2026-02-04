"""
Base model for email recipient parameters.

This model provides the standard parameters needed for email notifications.
It can be inherited by flow parameter models to avoid duplication.
"""
from pydantic import BaseModel, Field
from typing import List, ClassVar


class EmailRecipientParam(BaseModel):
    """Base model for email recipient parameters."""
    
    # Component hint for frontend grouping (Phase 2)
    _component_hint: ClassVar[str] = "EmailRecipientParam"
    
    email_to: List[str] = Field(
        default_factory=list,
        title="Email notification recipients",
        description="List of email addresses to send notifications to"
    )
