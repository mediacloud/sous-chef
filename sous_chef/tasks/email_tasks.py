"""
Email tasks for sous-chef flows.

Provides tasks for sending emails with template support.
"""
import os
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from jinja2 import Environment, FileSystemLoader, select_autoescape
from prefect import task
from prefect_email import EmailServerCredentials, email_send_message

from ..utils import get_logger


# Get the directory where this file is located
_TASKS_DIR = Path(__file__).parent
_TEMPLATES_DIR = _TASKS_DIR / "templates"

# Initialize Jinja2 environment for email templates
# Templates are loaded from the templates/ subdirectory
jinja_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATES_DIR)),
    autoescape=select_autoescape()
)


@task
def send_email(
    email_to: str | List[str],
    subject: str,
    msg: str,
    email_server_credentials_block: str = "email-password",
) -> Tuple[Dict[str, Any], None]:
    """
    Send an email using Prefect's email integration.
    
    Args:
        email_to: Single email address or list of email addresses
        subject: Email subject line
        msg: Email message body (plain text or HTML)
        email_server_credentials_block: Name of Prefect EmailServerCredentials block
        
    Returns:
        Dict with task result metadata
        
    Example:
        ```python
        send_email(
            email_to="user@example.com",
            subject="Test Email",
            msg="This is a test message"
        )
        ```
    """
    logger = get_logger()
    
    # Normalize email_to to a list
    if isinstance(email_to, str):
        email_addresses = [email_to]
    else:
        email_addresses = email_to
    
    email_server_credentials = EmailServerCredentials.load(email_server_credentials_block)
    logger.info(email_server_credentials)
    logger.info(f"email: {email_server_credentials.username }")
    logger.info(f"password: {email_server_credentials.password.get_secret_value()}")

    results = []
    for email_address in email_addresses:
        logger.info(f"Sending email to {email_address}")
        result = email_send_message.with_options(name=f"email {email_address}").submit(
            email_server_credentials=email_server_credentials,
            subject=subject,
            msg=msg,
            email_to=email_address,
        ).wait()
        results.append({
            "email": email_address,
            "task": result
        })
    
    result = {
        "emails_sent": len(results),
        "recipients": email_addresses,
        "subject": subject,
        "results": results
    }
    
    return result, None


@task
def send_templated_email(
    email_to: str | List[str],
    subject: str,
    template_name: str,
    template_data: Dict[str, Any],
    email_server_credentials_block: str = "email-password",
) -> Tuple[Dict[str, Any], None]:
    """
    Send an email using a Jinja2 template.
    
    Args:
        email_to: Single email address or list of email addresses
        subject: Email subject line
        template_name: Name of the template file in tasks/templates/ (e.g., "run_summary.j2")
        template_data: Dictionary of data to pass to the template for rendering
        email_server_credentials_block: Name of Prefect EmailServerCredentials block
        
    Returns:
        Dict with task result metadata
        
    Example:
        ```python
        send_templated_email(
            email_to="user@example.com",
            subject="Run Summary",
            template_name="run_summary.j2",
            template_data={"run_name": "test_run", "status": "success"}
        )
        ```
    """
    logger = get_logger()
    
    # Load and render template
    try:
        template = jinja_env.get_template(template_name)
        email_text = template.render(**template_data)
    except Exception as e:
        logger.error(f"Failed to load or render template '{template_name}': {e}")
        raise
    
    # Send email using the rendered template
    result, _ = send_email(
        email_to=email_to,
        subject=subject,
        msg=email_text,
        email_server_credentials_block=email_server_credentials_block,
    )
    return result, None


@task
def send_run_summary_email(
    email_to: str | List[str],
    run_data: Optional[Dict[str, Any]] = None,
    query_summary=None,
    b2_artifact=None,
    flow_name: Optional[str] = None,
    query: Optional[str] = None,
    subject: Optional[str] = None,
    template_name: Optional[str] = None,
    email_server_credentials_block: str = "email-password",
) -> Tuple[Dict[str, Any], None]:
    """
    Send a run summary email using a template.
    
    This is a convenience wrapper around send_templated_email specifically
    for sending flow execution summaries. Can handle both legacy run_data format
    and new artifact-based format.
    
    Args:
        email_to: Single email address or list of email addresses
        run_data: Dictionary containing run summary data (legacy format, optional)
        query_summary: MediacloudQuerySummary artifact or dict (optional)
        b2_artifact: FileUploadArtifact artifact or dict (optional)
        flow_name: Name of the flow that was executed (optional)
        query: The search query that was executed (optional)
        subject: Email subject line (auto-generated if not provided)
        template_name: Name of the template file (auto-selected if not provided)
        email_server_credentials_block: Name of Prefect EmailServerCredentials block
        
    Returns:
        Dict with task result metadata
        
    Example (legacy format):
        ```python
        send_run_summary_email(
            email_to=["user@example.com"],
            run_data={
                "run_name": "keywords_extraction",
                "status": "success",
                "article_count": 100
            }
        )
        ```
    
    Example (artifact format):
        ```python
        send_run_summary_email(
            email_to=["user@example.com"],
            query_summary=query_summary_artifact,
            b2_artifact=b2_artifact,
            flow_name="keywords_demo",
            query="climate change"
        )
        ```
    """
    logger = get_logger()
    
    # Convert artifacts to dicts if they're artifact objects
    query_summary_dict = None
    b2_artifact_dict = None
    
    if query_summary is not None:
        # Check if it's an artifact object with to_table_row method
        if hasattr(query_summary, 'to_table_row'):
            query_summary_dict = query_summary.to_table_row()
        elif isinstance(query_summary, dict):
            query_summary_dict = query_summary
        else:
            logger.warning(f"query_summary is not an artifact or dict, got {type(query_summary)}")
    
    if b2_artifact is not None:
        # Check if it's an artifact object with to_table_row method
        if hasattr(b2_artifact, 'to_table_row'):
            b2_artifact_dict = b2_artifact.to_table_row()
        elif isinstance(b2_artifact, dict):
            b2_artifact_dict = b2_artifact
        else:
            logger.warning(f"b2_artifact is not an artifact or dict, got {type(b2_artifact)}")
    
    # Determine template and build template_data
    if query_summary_dict is not None or b2_artifact_dict is not None:
        # Use new flow_results template
        if template_name is None:
            template_name = "flow_results.j2"
        
        # Build template data
        template_data = {}
        if query_summary_dict:
            template_data["query_summary"] = query_summary_dict
        if b2_artifact_dict:
            template_data["b2_artifact"] = b2_artifact_dict
        if flow_name:
            template_data["flow_name"] = flow_name
        if query:
            template_data["query"] = query
        
        # Generate subject if not provided
        if subject is None:
            if query:
                query_preview = query[:50] + "..." if len(query) > 50 else query
                subject = f"Sous-Chef Results: {query_preview}"
            else:
                subject = "Sous-Chef Flow Results"
    else:
        # Use legacy run_data format
        if template_name is None:
            template_name = "run_summary.j2"
        
        if run_data is None:
            run_data = {}
        
        template_data = run_data
        
        if subject is None:
            subject = "Sous-Chef Execution Summary"
    
    result, _ = send_templated_email(
        email_to=email_to,
        subject=subject,
        template_name=template_name,
        template_data=template_data,
        email_server_credentials_block=email_server_credentials_block,
    )
    return result, None