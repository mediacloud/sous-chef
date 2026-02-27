"""
Secret management for sous-chef tasks.

Secrets are loaded from:
1. Prefect blocks (when running in Prefect)
2. Environment variables (fallback for local/testing)
"""
from prefect.blocks.system import Secret
from prefect_aws import AwsCredentials
from prefect.context import TaskRunContext
from prefect import task
from .utils import get_logger
from prefect.variables import Variable
import os
from typing import Optional
import boto3
from botocore.config import Config

def get_mediacloud_api_key(block_name: str = "mediacloud-api-key") -> str:
    """
    Get MediaCloud API key from Prefect block or environment.
    
    Args:
        block_name: Name of Prefect secret block
        
    Returns:
        API key string
        
    Raises:
        ValueError: If key not found
    """
    # Try Prefect context first
    try:
        context = TaskRunContext.get()
        if context:
            secret = Secret.load(block_name)
            return secret.get()
    except Exception:
        pass
    
    # Fallback to environment variable
    env_var = block_name.upper().replace("-", "_")
    key = os.getenv(env_var)
    if key:
        return key
    
    raise ValueError(
        f"MediaCloud API key not found. "
        f"Set Prefect block '{block_name}' or environment variable '{env_var}'"
    )


def get_llm_api_key(
    provider: str = "huggingface",
    block_name: Optional[str] = None,
) -> str:
    """
    Get an LLM provider API key from Prefect or environment.

    Resolution order:
      1. Prefect Secret block (preferred when running in Prefect)
      2. Provider-specific env var (e.g., HUGGINGFACE_API_KEY, OPENAI_API_KEY)
      3. Generic env var LLM_PROVIDER_KEY

    Args:
        provider: LLM provider identifier (e.g. "huggingface", "openai").
        block_name: Optional Prefect Secret block name. If not provided,
                    defaults to f"llm-{provider}-api-key".

    Raises:
        ValueError if no key is found.
    """
    # 1) Prefect Secret block
    resolved_block = block_name or f"llm-{provider}-api-key"
    try:
        context = TaskRunContext.get()
        if context:
            secret = Secret.load(resolved_block)
            return secret.get()
    except Exception:
        pass

    # 2) Provider-specific env var
    provider_env_map = {
        "huggingface": "HUGGINGFACE_API_KEY",
        "openai": "OPENAI_API_KEY",
    }
    env_var = provider_env_map.get(provider)
    if env_var:
        key = os.getenv(env_var)
        if key:
            return key

    # 3) Generic fallback
    generic_var = "LLM_PROVIDER_KEY"
    key = os.getenv(generic_var)
    if key:
        return key

    raise ValueError(
        "LLM API key not found. Tried:\n"
        f"- Prefect Secret block '{resolved_block}'\n"
        f"- Provider env var '{env_var}'\n"
        f"- Generic env var '{generic_var}'"
    )

def get_aws_credentials(block_name: str = "aws-s3-credentials"):
    """Get AWS credentials from Prefect block or environment."""
    try:
        context = TaskRunContext.get()
        if context:
            return AwsCredentials.load(block_name)
    except Exception:
        pass
    
    # Fallback: try to construct from env vars
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        # Minimal credentials object for direct boto3 use
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        return session
    
    raise ValueError(f"AWS credentials not found in block '{block_name}' or environment")

@task
def get_b2_s3_client(block_name: str = "b2-s3-credentials"):
    """
    Get a boto3 S3 client configured for Backblaze B2's S3-compatible API.
    
    Order of resolution:
    1. Prefect AwsCredentials block named `block_name` (when in a Prefect context)
    2. Environment variables:
       - B2_KEY_ID
       - B2_APP_KEY
       - B2_S3_ENDPOINT  (e.g., https://s3.us-west-004.backblazeb2.com)
       - B2_REGION       ()
    """
    endpoint_url = os.getenv("B2_S3_ENDPOINT")
    region_name = os.getenv("B2_REGION", "us-east-005")
    logger = get_logger()

    # Try Prefect AwsCredentials block first (preferred when running under Prefect)
    try:
        aws_creds = AwsCredentials.load(block_name)


        session = aws_creds.get_boto3_session()
        params = aws_creds.aws_client_parameters.get_params_override() if aws_creds.aws_client_parameters else {}

        client_config = Config(
            signature_version="s3v4",
            s3={"addressing_style":"path"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required")
        return session.client("s3", config=client_config, **params)
    except Exception as e:
        # Log and fall through to env vars
        logger.info(f"Could not load Prefect block {block_name}: {e}")
        pass

    # Fallback to raw env vars for local/dev use
    key_id = os.getenv("B2_KEY_ID")
    app_key = os.getenv("B2_APP_KEY")

    if not key_id or not app_key:
        raise ValueError(
            "Backblaze B2 credentials not found. "
            "Either configure a Prefect AwsCredentials block "
            f"named '{block_name}' or set env vars B2_KEY_ID and B2_APP_KEY."
        )

    session = boto3.Session(
        aws_access_key_id=key_id,
        aws_secret_access_key=app_key,
        region_name=region_name,
    )
    logger.info("[B2 Client] Using environment variables (fallback)")
    logger.info(f"[B2 Client] Endpoint: {endpoint_url}")
    logger.info(f"[B2 Client] Region: {region_name}")
    logger.info(f"[B2 Client] Access key ID (first 10): {key_id[:10]}...")

    client = session.client("s3", endpoint_url=endpoint_url, region_name=region_name)
    
    logger.info(f"[B2 Client] Created client with endpoint: {client.meta.endpoint_url}")
    logger.info(f"[B2 Client] Created client with region: {client.meta.region_name}")
    
    return client

def get_b2_endpoint_url(block_name: str = "b2-s3-credentials") -> Optional[str]:
    """
    Get the B2 S3 endpoint URL from Prefect block or environment.
    
    Args:
        block_name: Name of Prefect AwsCredentials block
        
    Returns:
        Endpoint URL string, or None if not found
    """
    # Try Prefect AwsCredentials block first
    try:
        context = TaskRunContext.get()
        if context:
            aws_creds = AwsCredentials.load(block_name)
            # Get endpoint_url from aws_client_parameters
            endpoint_url = aws_creds.aws_client_parameters.endpoint_url
            if endpoint_url:
                return endpoint_url
    except Exception:
        pass
    
    # Fallback to environment variable
    return os.getenv("B2_S3_ENDPOINT")

def get_b2_bucket_name(variable_name: str = "b2-bucket-name") -> str:
    """
    Get the B2 bucket name from Prefect variable or environment.
    
    Order of resolution:
    1. Prefect Variable named `variable_name` (when in a Prefect context)
    2. Environment variable B2_BUCKET (fallback for local/testing)
    
    Args:
        variable_name: Name of Prefect variable (default: "b2-bucket-name")
        
    Returns:
        Bucket name string
        
    Raises:
        ValueError: If bucket name not found in variable or environment
    """
    # Try Prefect Variable first (preferred when running under Prefect)
    try:
        context = TaskRunContext.get()
        if context:
            bucket_name = Variable.get(variable_name)
            if bucket_name:
                return bucket_name
    except Exception:
        # Variable not found or error accessing it, fall through to env var
        pass
    
    # Fallback to environment variable
    bucket_name = os.getenv("B2_BUCKET")
    if bucket_name:
        return bucket_name
    
    # Default fallback
    return "sous-chef-output"