"""
Delllo — MinIO Object Storage Client
Handles all raw document storage operations.
"""

import io
import hashlib
from pathlib import Path
from typing import Optional

from minio import Minio
from minio.error import S3Error

from app.config import settings


def get_minio_client() -> Minio:
    return Minio(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def upload_document(
    file_bytes: bytes,
    filename: str,
    mime_type: str,
    tenant_id: str,
    document_id: str,
) -> tuple[str, str]:
    """
    Upload raw document to MinIO.
    Returns (storage_uri, sha256_checksum).
    Path: delllo-documents/{tenant_id}/{document_id}/{filename}
    """
    client = get_minio_client()
    bucket = settings.minio_bucket_documents
    ensure_bucket(client, bucket)

    checksum = hashlib.sha256(file_bytes).hexdigest()
    object_name = f"{tenant_id}/{document_id}/{filename}"

    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=io.BytesIO(file_bytes),
        length=len(file_bytes),
        content_type=mime_type,
        metadata={"x-checksum-sha256": checksum},
    )

    storage_uri = f"{bucket}/{object_name}"
    return storage_uri, checksum


def download_document(storage_uri: str) -> bytes:
    """Download raw bytes given a storage_uri like 'bucket/path/file.pdf'."""
    client = get_minio_client()
    parts = storage_uri.split("/", 1)
    bucket, object_name = parts[0], parts[1]
    response = client.get_object(bucket, object_name)
    return response.read()


def delete_document(storage_uri: str) -> None:
    client = get_minio_client()
    parts = storage_uri.split("/", 1)
    bucket, object_name = parts[0], parts[1]
    client.remove_object(bucket, object_name)
