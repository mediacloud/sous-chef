"""
Base model for webhook callback parameters.

This model provides the standard parameters needed for webhook notifications.
It can be inherited by flow parameter models to avoid duplication.

When a flow that includes this parameter is executed via the Kitchen API,
a POST request will be sent to the provided webhook_url upon completion
with run status and artifact metadata.
"""
from pydantic import BaseModel, Field, HttpUrl
from typing import Optional, ClassVar


class WebhookCallbackParam(BaseModel):
    """Base model for webhook callback parameters."""
    
    # Component hint for frontend grouping
    _component_hint: ClassVar[str] = "WebhookCallbackParam"
    
    webhook_url: Optional[HttpUrl] = Field(
        default=None,
        title="Webhook callback URL",
        description="Optional URL to receive a POST notification when the flow completes. "
                    "The webhook will receive a JSON payload with run_id, status, and artifacts. "
                    "If not provided, no webhook will be sent."
    )
    
    webhook_secret: Optional[str] = Field(
        default=None,
        title="Webhook secret",
        description="Optional secret token for webhook authentication. "
                    "If provided, this will be sent as the 'X-Webhook-Secret' header "
                    "in the webhook POST request."
    )
