"""
Export tasks for sous-chef-two.

Currently provides:
- csv_to_b2: upload a pandas DataFrame as a CSV to Backblaze B2 using the
  S3-compatible API.
"""
import os
from datetime import date
from io import BytesIO
from typing import Dict, Any
import tempfile

import pandas as pd
from prefect import task

from ..secrets import get_b2_s3_client, get_b2_endpoint_url, get_b2_bucket_name
from ..artifacts import ArtifactResult, FileUploadArtifact
from ..utils import get_logger, is_test_mode


def _b2_credentials_available(block_name: str = "b2-s3-credentials") -> bool:
    """
    Check if B2 credentials are available without raising an error.
    
    This is used to automatically enable dry_run mode when credentials
    are not available, allowing flows to run in test environments.
    
    Checks for credentials in this order:
    1. Prefect block (if in Prefect context)
    2. Environment variables (B2_KEY_ID and B2_APP_KEY)
    
    Args:
        block_name: Name of the Prefect block or environment variable prefix
        
    Returns:
        True if credentials are available, False otherwise
    """
    import os
    from prefect.context import TaskRunContext
    
    # First check environment variables (fastest check)
    key_id = os.getenv("B2_KEY_ID")
    app_key = os.getenv("B2_APP_KEY")
    if key_id and app_key:
        return True
    
    # Then check Prefect block (if in Prefect context)
    try:
        context = TaskRunContext.get()
        if context:
            from prefect_aws import AwsCredentials
            # Try to load the block - if it exists, credentials are available
            AwsCredentials.load(block_name)
            return True
    except Exception:
        pass
    
    # No credentials found
    return False


@task
def csv_to_b2(
    df: pd.DataFrame,
    object_name: str,
    add_date_slug: bool = True,
    ensure_unique: bool = True,
    b2_block_name: str = "b2-s3-credentials",
    dry_run: bool = False,
    auto_dry_run_on_missing_creds: bool = True,
) -> ArtifactResult[Dict[str, Any]]:
    """
    Upload a DataFrame as a CSV to Backblaze B2 (S3-compatible).

    The bucket name is automatically retrieved from Prefect variable "b2-bucket-name"
    or the B2_BUCKET environment variable, defaulting to "sous-chef-output".

    Args:
        df: DataFrame to export.
        object_name: Object key/path. If it contains the substring ``\"DATE\"``
            and ``add_date_slug`` is True, ``\"DATE\"`` will be replaced with
            the current date (YYYY-MM-DD).
        add_date_slug: Whether to replace ``\"DATE\"`` in the object name with a
            date string.
        ensure_unique: If True, will probe for existing objects and append
            ``-0``, ``-1``, ... until a free name is found.
        b2_block_name: Name of the Prefect AwsCredentials block configured with
            B2 credentials (used when running under Prefect).
        dry_run: If True, do not actually upload; just compute the final object
            name and return metadata. Useful for tests.
        auto_dry_run_on_missing_creds: If True, automatically use dry_run mode
            when B2 credentials are not available or when test mode is enabled.
            This allows flows to run in test environments without crashing.
            Default: True

    Returns:
        ArtifactResult[Dict[str, Any]]: Tuple of (metadata_dict, FileUploadArtifact)
        
        - First element: Dict with:
            - bucket: bucket name
            - object: final object key
            - url: best-effort HTTPS URL to the object (may be None if endpoint not set)
            - columns_saved: list of DataFrame column names
        - Second element: FileUploadArtifact with upload details for frontend display
        
    Example:
        metadata, artifact = csv_to_b2(df, "output.csv")
    """
    if df is None:
        raise ValueError("csv_to_b2: DataFrame 'df' must not be None")

    logger = get_logger()
    
    # Auto-enable dry_run in test mode
    if is_test_mode() and not dry_run:
        logger.info("[CSVToB2] Test mode detected - using dry_run")
        dry_run = True
    
    # Auto-detect missing credentials and switch to dry_run
    if not dry_run and auto_dry_run_on_missing_creds:
        if not _b2_credentials_available(block_name=b2_block_name):
            logger.warning(
                f"[CSVToB2] B2 credentials not available. "
                f"Switching to dry_run mode for testing. "
                f"Set auto_dry_run_on_missing_creds=False to disable this behavior."
            )
            dry_run = True
    
    # Get bucket name from Prefect variable or environment
    bucket_name = get_b2_bucket_name()
    # Build CSV into an in-memory buffer
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8', lineterminator='\n')
    csv_buffer.seek(0)


    # Insert date slug if requested
    final_object_name = object_name
    if add_date_slug and "DATE" in final_object_name:
        datestring = date.today().strftime("%Y-%m-%d")
        final_object_name = final_object_name.replace("DATE", datestring)

    #We think this is the fix?
    final_object_name = final_object_name.lstrip('/')
    client = None
    endpoint_url = get_b2_endpoint_url(block_name=b2_block_name)

    if not dry_run:
        client = get_b2_s3_client(block_name=b2_block_name)

    put_name = final_object_name

    if ensure_unique and not dry_run:
        # Probe for an available object name by appending -0, -1, ...
        base, ext = (
            final_object_name.rsplit(".", 1)
            if "." in final_object_name
            else (final_object_name, "")
        )
        index = 0
        while True:
            candidate = f"{base}-{index}.{ext}" if ext else f"{base}-{index}"
            try:
                client.head_object(Bucket=bucket_name, Key=candidate)
                index += 1
            except Exception:
                put_name = candidate
                break
    else:
        put_name = final_object_name

    # Add test prefix to object name when in dry_run mode
    if dry_run and not put_name.startswith("test-"):
        put_name = f"test-{put_name}"

    if not dry_run:
        # Log detailed client configuration before put_object
        logger.info(f"[CSVToB2] Preparing to upload CSV")
        logger.info(f"[CSVToB2] Bucket: {bucket_name}, Key: {put_name}")
        logger.info(f"[CSVToB2] Client endpoint: {client.meta.endpoint_url}")
        logger.info(f"[CSVToB2] Client region: {client.meta.region_name}")
        
        logger.info(f"[CSVToB2] CSV buffer size: {len(csv_buffer.getvalue())} bytes")
        
        client.put_object(
            Body=csv_buffer,#.getvalue(),
            Bucket=bucket_name,
            Key=put_name,
            ContentType="text/csv"
            #ContentLength=len(csv_buffer.getvalue())
        )
        
        logger.info(f"[CSVToB2] Successfully uploaded to {bucket_name}/{put_name}")

    # Best-effort URL construction using the configured endpoint, if present.
    # In dry_run mode, don't create a URL
    url = None
    if endpoint_url and not dry_run:
        url = f"{endpoint_url.rstrip('/')}/{bucket_name}/{put_name}"

    # Create metadata dict (result)
    metadata = {
        "bucket": bucket_name,
        "object": put_name,
        "url": url,
        "columns_saved": list(df.columns),
        "test_mode": dry_run,  # Flag to indicate this is a test/dry_run artifact
    }
    
    # Create artifact for frontend display
    artifact = None
    if not dry_run:
        artifact = FileUploadArtifact(
            url=url,
            bucket=bucket_name,
            object_key=put_name,
            file_type="csv",
            columns_saved=list(df.columns),
            row_count=len(df)
        )
    else:
        local_file_name = f"test-{put_name}"

        tmp_dir = tempfile.gettempdir()
        final_dir = "/".join(local_file_name.split("/")[:-1])
        os.makedirs(tmp_dir+"/"+final_dir, exist_ok=True)
        local_path = f"{tmp_dir}/{local_file_name}"
        df.to_csv(local_path, index=False)
        logger.info(f"[CSVToB2] Dry run mode - CSV also saved locally to {local_path} for inspection")

    return metadata, artifact

