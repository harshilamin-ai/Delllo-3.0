"""
Delllo — Ingestion + Extraction API Endpoints

POST /v1/ingest/document          Upload a file (PDF, DOCX, TXT, MD)
POST /v1/ingest/text              Ingest raw text directly
POST /v1/ingest/{document_id}/extract   Run extraction on an ingested doc
POST /v1/ingest/pipeline          Full pipeline: ingest + extract in one call
GET  /v1/ingest/{document_id}     Get document status + chunk summary
"""

from uuid import UUID
from typing import Optional, Annotated

from fastapi import (
    APIRouter, Depends, File, Form, UploadFile,
    HTTPException, Query, BackgroundTasks,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db.postgres import get_db
from app.services.ingestion import ingest_document, SUPPORTED_TYPES
from app.services.extraction import extract_from_document
from app.schemas.ingestion import (
    DocumentUploadResponse,
    DocumentDetailResponse,
    ExtractionRequest,
    ExtractionResponse,
    ChunkOut,
)

router = APIRouter()


# ─────────────────────────────────────────────
#  POST /v1/ingest/document
#  Upload a file and ingest it
# ─────────────────────────────────────────────

@router.post(
    "/ingest/document",
    response_model=DocumentUploadResponse,
    summary="Upload and ingest a document (PDF, DOCX, TXT, MD)",
)
async def upload_document(
    file: Annotated[UploadFile, File(description="File to ingest")],
    tenant_id: Annotated[UUID, Form(description="Tenant UUID")],
    user_id: Annotated[UUID, Form(description="User UUID who owns this document")],
    source_type: Annotated[str, Form(description="cv | bio | paper | note | upload | meeting_note | chat")] = "upload",
    embed: Annotated[bool, Form(description="Generate vector embeddings for chunks")] = True,
    db: AsyncSession = Depends(get_db),
):
    """
    Full ingestion pipeline:
    1. Validate file type
    2. Parse text (PDF/DOCX/TXT/MD)
    3. Chunk into ~400 token segments
    4. Embed chunks via nomic-embed-text (optional)
    5. Store in MinIO + PostgreSQL
    6. Return document_id for extraction
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="File has no filename")

    # Validate file type
    mime_type = file.content_type or "application/octet-stream"
    if mime_type not in SUPPORTED_TYPES:
        # Try guessing from extension
        ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
        ext_map = {"pdf": "application/pdf", "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                   "txt": "text/plain", "md": "text/markdown"}
        mime_type = ext_map.get(ext, "text/plain")

    file_bytes = await file.read()

    if len(file_bytes) == 0:
        raise HTTPException(status_code=400, detail="File is empty")

    if len(file_bytes) > 50 * 1024 * 1024:  # 50MB limit
        raise HTTPException(status_code=413, detail="File exceeds 50MB limit")

    result = await ingest_document(
        db,
        file_bytes=file_bytes,
        filename=file.filename,
        mime_type=mime_type,
        tenant_id=str(tenant_id),
        user_id=str(user_id),
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
#  Ingest raw text directly (no file upload)
# ─────────────────────────────────────────────

@router.post(
    "/ingest/text",
    response_model=DocumentUploadResponse,
    summary="Ingest raw text directly (chat answers, onboarding prompts)",
)
async def ingest_text(
    tenant_id: Annotated[UUID, Form()],
    user_id: Annotated[UUID, Form()],
    content: Annotated[str, Form(description="Raw text content to ingest")],
    source_type: Annotated[str, Form()] = "chat",
    filename: Annotated[str, Form()] = "direct_text.txt",
    embed: Annotated[bool, Form()] = True,
    db: AsyncSession = Depends(get_db),
):
    """Ingest a string of text directly — useful for chat answers, bios, onboarding prompts."""
    if not content.strip():
        raise HTTPException(status_code=400, detail="Content is empty")

    result = await ingest_document(
        db,
        file_bytes=content.encode("utf-8"),
        filename=filename,
        mime_type="text/plain",
        tenant_id=str(tenant_id),
        user_id=str(user_id),
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
#  Run extraction on an already-ingested document
# ─────────────────────────────────────────────

@router.post(
    "/ingest/{document_id}/extract",
    response_model=ExtractionResponse,
    summary="Run LLM extraction on an ingested document",
)
async def run_extraction(
    document_id: UUID,
    req: ExtractionRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Run the 8B LLM extraction pipeline on a previously ingested document.
    Extracts: skills, domains, objectives, offers, achievements.
    Writes results to extracted_facts table.
    """
    return await extract_from_document(
        db,
        document_id=str(document_id),
        user_id=str(req.user_id),
        tenant_id=str(req.tenant_id),
        source_type=req.source_type,
        force_reextract=req.force_reextract,
    )


# ─────────────────────────────────────────────
#  POST /v1/ingest/pipeline
#  Full pipeline: upload + extract in one call
# ─────────────────────────────────────────────

@router.post(
    "/ingest/pipeline",
    summary="Full pipeline: ingest file + run extraction immediately",
)
async def full_pipeline(
    file: Annotated[UploadFile, File()],
    tenant_id: Annotated[UUID, Form()],
    user_id: Annotated[UUID, Form()],
    source_type: Annotated[str, Form()] = "cv",
    embed: Annotated[bool, Form()] = True,
    db: AsyncSession = Depends(get_db),
):
    """
    One-shot endpoint: ingest a file AND run extraction.
    Returns both the ingestion result and the extracted facts.
    Perfect for testing the full pipeline end-to-end.
    """
    # Step 1: Ingest
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename")

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
        tenant_id=str(tenant_id),
        user_id=str(user_id),
        source_type=source_type,
        embed=embed,
    )

    if ingest_result.get("status") == "failed":
        raise HTTPException(status_code=422, detail=ingest_result.get("error"))

    document_id = ingest_result["document_id"]

    # Step 2: Extract
    extraction_result = await extract_from_document(
        db,
        document_id=document_id,
        user_id=str(user_id),
        tenant_id=str(tenant_id),
        source_type=source_type,
    )

    return {
        "ingestion": ingest_result,
        "extraction": extraction_result,
        "pipeline_status": "completed" if extraction_result.status == "completed" else "partial",
    }


# ─────────────────────────────────────────────
#  GET /v1/ingest/{document_id}
#  Document status + chunk preview
# ─────────────────────────────────────────────

@router.get(
    "/ingest/{document_id}",
    summary="Get document status and chunk summary",
)
async def get_document_status(
    document_id: UUID,
    include_chunks: bool = Query(default=False, description="Include chunk text previews"),
    db: AsyncSession = Depends(get_db),
):
    """Check the status of an ingested document."""
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
        {"doc_id": str(document_id)},
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
            {"doc_id": str(document_id)},
        )
        response["chunks"] = [dict(r) for r in chunks_result.mappings().all()]

    # Also pull extracted facts count
    facts_result = await db.execute(
        text("""
            SELECT fact_type, COUNT(*) as count
            FROM extracted_facts
            WHERE source_document_id = :doc_id
            GROUP BY fact_type
        """),
        {"doc_id": str(document_id)},
    )
    response["extracted_facts"] = {r["fact_type"]: r["count"] for r in facts_result.mappings().all()}

    return response
