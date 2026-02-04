"""
Email tasks for sous-chef flows.

Provides tasks for sending emails with template support.
"""
import os
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from jinja2 import Environment, FileSystemLoader, select_autoescape
from prefect import task
from prefect.logging import get_run_logger
from prefect_email import EmailServerCredentials, email_send_message


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
    logger = get_run_logger()
    
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
    logger = get_run_logger()
    
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
    run_data: Dict[str, Any],
    subject: str = "Sous-Chef Execution Summary",
    template_name: str = "run_summary.j2",
    email_server_credentials_block: str = "email-password",
) -> Tuple[Dict[str, Any], None]:
    """
    Send a run summary email using a template.
    
    This is a convenience wrapper around send_templated_email specifically
    for sending flow execution summaries.
    
    Args:
        email_to: Single email address or list of email addresses
        run_data: Dictionary containing run summary data (will be passed to template)
        subject: Email subject line (default: "Sous-Chef Execution Summary")
        template_name: Name of the template file (default: "run_summary.j2")
        email_server_credentials_block: Name of Prefect EmailServerCredentials block
        
    Returns:
        Dict with task result metadata
        
    Example:
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
    """
    result, _ = send_templated_email(
        email_to=email_to,
        subject=subject,
        template_name=template_name,
        template_data=run_data,
        email_server_credentials_block=email_server_credentials_block,
    )
    return result, None