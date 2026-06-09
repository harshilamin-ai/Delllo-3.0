"""
Delllo — Ingestion + Extraction API Endpoints

POST /v1/ingest/document          Upload a file (PDF, DOCX, TXT, MD)
POST /v1/ingest/text              Ingest raw text directly
POST /v1/ingest/{document_id}/extract   Run extraction on an ingested doc
POST /v1/ingest/pipeline          Full pipeline: ingest + extract in one call
GET  /v1/ingest/{document_id}     Get document status + chunk summary

ID FORMAT NOTE
──────────────
user_id  accepts UUID or 24-char MongoDB ObjectID.
tenant_id is OPTIONAL — users without a network are placed under a
           system-level "no-network" tenant (SYSTEM_TENANT_ID). When a
           tenant_id is supplied it also accepts UUID or MongoDB ObjectID.
"""

import re
import uuid as _uuid
from typing import Optional, Annotated

from fastapi import (
    APIRouter, Depends, File, Form, UploadFile,
    HTTPException, Query,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db.postgres import get_db
from app.services.ingestion import ingest_document, SUPPORTED_TYPES
from app.services.extraction import extract_from_document
from app.schemas.ingestion import (
    DocumentUploadResponse,
    ExtractionRequest,
    ExtractionResponse,
)

router = APIRouter()

# ─────────────────────────────────────────────
#  ID helpers (mirrors admin.py — no circular import)
# ─────────────────────────────────────────────

_UUID_RE  = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I
)
_MONGO_RE = re.compile(r'^[0-9a-f]{24}$', re.I)
_MONGO_NS = _uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

# Sentinel tenant for users who have not joined any network yet.
# This UUID is stable — do NOT change it after first deploy.
SYSTEM_TENANT_ID = "00000000-0000-0000-0000-000000000000"


def _is_valid_id(val: str) -> bool:
    return bool(_UUID_RE.match(val) or _MONGO_RE.match(val))


def _norm_id(val: str, field: str = "id") -> str:
    """Return a UUID string from UUID or MongoDB ObjectID. Raises 400 on bad input."""
    if not _is_valid_id(val):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {field} format: '{val}'. "
                   "Expected a UUID or 24-char MongoDB ObjectID.",
        )
    if _UUID_RE.match(val):
        return val
    return str(_uuid.uuid5(_MONGO_NS, val))


async def _resolve_tenant(
    tenant_id: Optional[str],
    db: AsyncSession,
) -> str:
    """
    Resolve the effective tenant UUID:
    - None / empty  → SYSTEM_TENANT_ID (no-network user)
    - MongoDB ID    → deterministic UUID-v5
    - UUID          → passed through

    Ensures the system tenant row exists on first use.
    """
    if not tenant_id:
        tid = SYSTEM_TENANT_ID
    else:
        tid = _norm_id(tenant_id, "tenant_id")

    # Auto-create the tenant row if it doesn't exist yet
    await db.execute(
        text("""
            INSERT INTO tenants (tenant_id, name, slug, status)
            VALUES (:tid, :name, :slug, 'active')
            ON CONFLICT (tenant_id) DO NOTHING
        """),
        {
            "tid":  tid,
            "name": "System (no network)" if tid == SYSTEM_TENANT_ID else f"Tenant {tid[:8]}",
            "slug": "system-no-network"   if tid == SYSTEM_TENANT_ID else f"auto-{tid[:8]}",
        },
    )
    return tid


# ─────────────────────────────────────────────
#  POST /v1/ingest/document
# ─────────────────────────────────────────────

@router.post(
    "/ingest/document",
    response_model=DocumentUploadResponse,
    summary="Upload and ingest a document (PDF, DOCX, TXT, MD)",
)
async def upload_document(
    file:       Annotated[UploadFile, File(description="File to ingest")],
    user_id:    Annotated[str,  Form(description="User ID (UUID or MongoDB ObjectID)")],
    tenant_id:  Annotated[Optional[str], Form(description="Network/tenant ID — omit if user has no network")] = None,
    source_type: Annotated[str, Form(description="cv | bio | paper | note | upload | meeting_note | chat")] = "upload",
    embed:      Annotated[bool, Form(description="Generate vector embeddings for chunks")] = True,
    db: AsyncSession = Depends(get_db),
):
    """
    Full ingestion pipeline:
    1. Validate file type
    2. Parse text (PDF / DOCX / TXT / MD)
    3. Chunk into ~400-token segments
    4. Embed chunks via nomic-embed-text (optional)
    5. Store in MinIO + PostgreSQL
    6. Return document_id (use for extraction next)

    tenant_id is optional. Users who have not joined a network are
    filed under the system tenant and will be matched once they join.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="File has no filename")

    uid = _norm_id(user_id, "user_id")
    tid = await _resolve_tenant(tenant_id, db)

    mime_type = file.content_type or "application/octet-stream"
    if mime_type not in SUPPORTED_TYPES:
        ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
        ext_map = {
            "pdf":  "application/pdf",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "txt":  "text/plain",
            "md":   "text/markdown",
        }
        mime_type = ext_map.get(ext, "text/plain")

    file_bytes = await file.read()
    if len(file_bytes) == 0:
        raise HTTPException(status_code=400, detail="File is empty")
    if len(file_bytes) > 50 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File exceeds 50MB limit")

    result = await ingest_document(
        db,
        file_bytes=file_bytes,
        filename=file.filename,
        mime_type=mime_type,
        tenant_id=tid,
        user_id=uid,
        source_type=source_type,
        embed=embed,
    )

    if result.get("status") == "failed":
        raise HTTPException(status_code=422, detail=result.get("error", "Ingestion failed"))

    return DocumentUploadResponse(
        document_id=result["document_id"],
        filename=result["filename"],
        source_type=result["source_type"],
        status=result["status"],
        chunk_count=result["chunk_count"],
        storage_uri=result["storage_uri"],
        message=result["message"],
    )


# ─────────────────────────────────────────────
#  POST /v1/ingest/text
# ─────────────────────────────────────────────

@router.post(
    "/ingest/text",
    response_model=DocumentUploadResponse,
    summary="Ingest raw text directly (chat answers, onboarding prompts)",
)
async def ingest_text(
    user_id:    Annotated[str,  Form(description="User ID (UUID or MongoDB ObjectID)")],
    content:    Annotated[str,  Form(description="Raw text content to ingest")],
    tenant_id:  Annotated[Optional[str], Form(description="Network/tenant ID — omit if user has no network")] = None,
    source_type: Annotated[str, Form()] = "chat",
    filename:   Annotated[str,  Form()] = "direct_text.txt",
    embed:      Annotated[bool, Form()] = True,
    db: AsyncSession = Depends(get_db),
):
    """Ingest a string of text directly — useful for chat answers, bios, onboarding prompts."""
    if not content.strip():
        raise HTTPException(status_code=400, detail="Content is empty")

    uid = _norm_id(user_id, "user_id")
    tid = await _resolve_tenant(tenant_id, db)

    result = await ingest_document(
        db,
        file_bytes=content.encode("utf-8"),
        filename=filename,
        mime_type="text/plain",
        tenant_id=tid,
        user_id=uid,
        source_type=source_type,
        embed=embed,
    )

    return DocumentUploadResponse(
        document_id=result["document_id"],
        filename=result["filename"],
        source_type=result["source_type"],
        status=result["status"],
        chunk_count=result["chunk_count"],
        storage_uri=result["storage_uri"],
        message=result["message"],
    )


# ─────────────────────────────────────────────
#  POST /v1/ingest/{document_id}/extract
# ─────────────────────────────────────────────

@router.post(
    "/ingest/{document_id}/extract",
    response_model=ExtractionResponse,
    summary="Run LLM extraction on an ingested document",
)
async def run_extraction(
    document_id: str,
    req: ExtractionRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Run the LLM extraction pipeline on a previously ingested document.
    Extracts: skills, domains, topics, needs, objectives, offers,
              achievements, assets, projects, locations, constraints.
    Writes results to extracted_facts and mirrors to iKG (Memgraph).
    """
    doc_id = _norm_id(document_id, "document_id")
    return await extract_from_document(
        db,
        document_id=doc_id,
        user_id=str(req.user_id),
        tenant_id=str(req.tenant_id),
        source_type=req.source_type,
        force_reextract=req.force_reextract,
    )


# ─────────────────────────────────────────────
#  POST /v1/ingest/pipeline
# ─────────────────────────────────────────────

@router.post(
    "/ingest/pipeline",
    summary="Full pipeline: ingest file + run extraction immediately",
)
async def full_pipeline(
    file:       Annotated[UploadFile, File()],
    user_id:    Annotated[str,  Form(description="User ID (UUID or MongoDB ObjectID)")],
    tenant_id:  Annotated[Optional[str], Form(description="Network/tenant ID — omit if user has no network")] = None,
    source_type: Annotated[str, Form()] = "cv",
    embed:      Annotated[bool, Form()] = True,
    db: AsyncSession = Depends(get_db),
):
    """
    One-shot endpoint: ingest a file AND run LLM extraction.
    Returns both the ingestion result and the extracted facts.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename")

    uid = _norm_id(user_id, "user_id")
    tid = await _resolve_tenant(tenant_id, db)

    mime_type = file.content_type or "application/octet-stream"
    ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    ext_map = {
        "pdf":  "application/pdf",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "txt":  "text/plain",
        "md":   "text/markdown",
    }
    if mime_type == "application/octet-stream":
        mime_type = ext_map.get(ext, "text/plain")

    file_bytes = await file.read()
    if not file_bytes:
        raise HTTPException(status_code=400, detail="File is empty")

    ingest_result = await ingest_document(
        db,
        file_bytes=file_bytes,
        filename=file.filename,
        mime_type=mime_type,
        tenant_id=tid,
        user_id=uid,
        source_type=source_type,
        embed=embed,
    )

    if ingest_result.get("status") == "failed":
        raise HTTPException(status_code=422, detail=ingest_result.get("error"))

    document_id = ingest_result["document_id"]

    extraction_result = await extract_from_document(
        db,
        document_id=document_id,
        user_id=uid,
        tenant_id=tid,
        source_type=source_type,
    )

    return {
        "ingestion":       ingest_result,
        "extraction":      extraction_result,
        "pipeline_status": "completed" if extraction_result.status == "completed" else "partial",
    }


# ─────────────────────────────────────────────
#  GET /v1/ingest/{document_id}
# ─────────────────────────────────────────────

@router.get(
    "/ingest/{document_id}",
    summary="Get document status and chunk summary",
)
async def get_document_status(
    document_id: str,
    include_chunks: bool = Query(default=False, description="Include chunk text previews"),
    db: AsyncSession = Depends(get_db),
):
    """Check the status of an ingested document."""
    doc_id = _norm_id(document_id, "document_id")

    doc_result = await db.execute(
        text("""
            SELECT d.document_id, d.filename, d.source_type, d.status,
                   d.storage_uri, d.created_at,
                   COUNT(dc.chunk_id) AS chunk_count
            FROM documents d
            LEFT JOIN document_chunks dc ON dc.document_id = d.document_id
            WHERE d.document_id = :doc_id
            GROUP BY d.document_id, d.filename, d.source_type,
                     d.status, d.storage_uri, d.created_at
        """),
        {"doc_id": doc_id},
    )
    doc = doc_result.mappings().first()
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    response = dict(doc)

    if include_chunks:
        chunks_result = await db.execute(
            text("""
                SELECT chunk_id, chunk_index, token_count,
                       LEFT(text, 200) AS text_preview
                FROM document_chunks
                WHERE document_id = :doc_id
                ORDER BY chunk_index
            """),
            {"doc_id": doc_id},
        )
        response["chunks"] = [dict(r) for r in chunks_result.mappings().all()]

    facts_result = await db.execute(
        text("""
            SELECT fact_type, COUNT(*) as count
            FROM extracted_facts
            WHERE source_document_id = :doc_id
            GROUP BY fact_type
        """),
        {"doc_id": doc_id},
    )
    response["extracted_facts"] = {
        r["fact_type"]: r["count"]
        for r in facts_result.mappings().all()
    }

    return response