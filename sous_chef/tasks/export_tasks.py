"""
Export tasks for sous-chef-two.

Currently provides:
- csv_to_b2: upload a pandas DataFrame as a CSV to Backblaze B2 using the
  S3-compatible API.
"""
from datetime import date
from io import BytesIO
from typing import Dict, Any

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger

from ..secrets import get_b2_s3_client, get_b2_endpoint_url


@task
def csv_to_b2(
    df: pd.DataFrame,
    bucket_name: str,
    object_name: str,
    add_date_slug: bool = True,
    ensure_unique: bool = True,
    normalize_name: bool = True,
    b2_block_name: str = "b2-s3-credentials",
    dry_run: bool = False,
) -> Dict[str, Any]:

    """
    Upload a DataFrame as a CSV to Backblaze B2 (S3-compatible).

    Args:
        df: DataFrame to export.
        bucket_name: Target B2 bucket name.
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

    Returns:
        Dict with:
        - bucket: bucket name
        - object: final object key
        - url: best-effort HTTPS URL to the object (may be None if endpoint not set)
        - columns_saved: list of DataFrame column names
    """
    if df is None:
        raise ValueError("csv_to_b2: DataFrame 'df' must not be None")

    logger = get_run_logger()
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
    url = None
    if endpoint_url:
        url = f"{endpoint_url.rstrip('/')}/{bucket_name}/{put_name}"

    return {
        "bucket": bucket_name,
        "object": put_name,
        "url": url,
        "columns_saved": list(df.columns),
    }

