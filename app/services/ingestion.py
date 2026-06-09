"""
Delllo RAIN3.0 — Ingestion Service
─────────────────────────────────────────────────────────────────
Pipeline:
  1. Receive file bytes + metadata
  2. Detect mime type, validate format
  3. Parse text from PDF / DOCX / TXT / Markdown / plain text
  4. Chunk into ~400 token segments with 50 token overlap
  5. Store raw file in MinIO
  6. Write Document row + DocumentChunk rows to PostgreSQL
  7. Return document_id (triggers extraction next)
─────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import io
import re
import uuid
import logging
from pathlib import Path
from typing import Optional

import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.config import settings
from app.db.storage import upload_document

logger = logging.getLogger(__name__)

# ── Supported MIME types ──────────────────────────────────────────
SUPPORTED_TYPES = {
    "application/pdf":                        "pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "text/plain":                             "txt",
    "text/markdown":                          "md",
    "text/x-markdown":                        "md",
    "application/octet-stream":               "txt",   # fallback
}

# ── Chunking config ───────────────────────────────────────────────
CHUNK_SIZE_TOKENS   = 400
CHUNK_OVERLAP_CHARS = 200   # character overlap between chunks
AVG_CHARS_PER_TOKEN = 4     # rough estimate for splitting


# ─────────────────────────────────────────────
#  TEXT EXTRACTION (by file type)
# ─────────────────────────────────────────────

def _extract_text_from_pdf(file_bytes: bytes) -> str:
    """Extract raw text from PDF using pypdf."""
    try:
        import pypdf
        reader = pypdf.PdfReader(io.BytesIO(file_bytes))
        pages = []
        for page in reader.pages:
            extracted = page.extract_text() or ""
            pages.append(extracted)
        return "\n\n".join(pages)
    except Exception as e:
        logger.warning(f"PDF extraction failed, trying fallback: {e}")
        # Fallback: treat as raw text
        return file_bytes.decode("utf-8", errors="ignore")


def _extract_text_from_docx(file_bytes: bytes) -> str:
    """Extract raw text from DOCX using python-docx."""
    try:
        import docx
        doc = docx.Document(io.BytesIO(file_bytes))
        paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
        return "\n\n".join(paragraphs)
    except Exception as e:
        logger.warning(f"DOCX extraction failed: {e}")
        return file_bytes.decode("utf-8", errors="ignore")


def _extract_text_from_txt(file_bytes: bytes) -> str:
    """Plain text — just decode."""
    for encoding in ("utf-8", "latin-1", "cp1252"):
        try:
            return file_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    return file_bytes.decode("utf-8", errors="replace")


def extract_text(file_bytes: bytes, mime_type: str) -> str:
    """
    Dispatch to the correct text extractor based on MIME type.
    Returns clean UTF-8 text ready for chunking.
    """
    file_type = SUPPORTED_TYPES.get(mime_type, "txt")

    if file_type == "pdf":
        raw = _extract_text_from_pdf(file_bytes)
    elif file_type == "docx":
        raw = _extract_text_from_docx(file_bytes)
    else:
        raw = _extract_text_from_txt(file_bytes)

    # Normalise whitespace — collapse 3+ newlines to 2
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    raw = re.sub(r"[ \t]+", " ", raw)
    return raw.strip()


# ─────────────────────────────────────────────
#  CHUNKING
# ─────────────────────────────────────────────

def _estimate_tokens(text: str) -> int:
    return max(1, len(text) // AVG_CHARS_PER_TOKEN)


def chunk_text(text: str) -> list[dict]:
    """
    Split text into overlapping chunks.
    Each chunk is a dict: {index, text, token_count, metadata}
    Strategy:
      - Split on paragraph breaks (\n\n) first to keep semantics intact
      - If a paragraph is too big, split on sentence boundaries
      - Combine small paragraphs until we hit CHUNK_SIZE_TOKENS
      - Slide a CHUNK_OVERLAP_CHARS overlap window between chunks
    """
    if not text.strip():
        return []

    # Split into paragraphs
    paragraphs = [p.strip() for p in re.split(r"\n\n+", text) if p.strip()]

    chunks = []
    current_chunk = ""
    chunk_index = 0

    for para in paragraphs:
        # If adding this paragraph keeps us under limit, accumulate
        candidate = (current_chunk + "\n\n" + para).strip() if current_chunk else para

        if _estimate_tokens(candidate) <= CHUNK_SIZE_TOKENS:
            current_chunk = candidate
        else:
            # Flush current chunk
            if current_chunk:
                chunks.append({
                    "index": chunk_index,
                    "text": current_chunk,
                    "token_count": _estimate_tokens(current_chunk),
                    "metadata": {"chunk_strategy": "paragraph"},
                })
                chunk_index += 1

            # If single paragraph is oversized, split by sentences
            if _estimate_tokens(para) > CHUNK_SIZE_TOKENS:
                sentences = re.split(r"(?<=[.!?])\s+", para)
                current_chunk = ""
                for sent in sentences:
                    candidate = (current_chunk + " " + sent).strip() if current_chunk else sent
                    if _estimate_tokens(candidate) <= CHUNK_SIZE_TOKENS:
                        current_chunk = candidate
                    else:
                        if current_chunk:
                            chunks.append({
                                "index": chunk_index,
                                "text": current_chunk,
                                "token_count": _estimate_tokens(current_chunk),
                                "metadata": {"chunk_strategy": "sentence"},
                            })
                            chunk_index += 1
                        current_chunk = sent
            else:
                current_chunk = para

    # Flush last chunk
    if current_chunk.strip():
        chunks.append({
            "index": chunk_index,
            "text": current_chunk,
            "token_count": _estimate_tokens(current_chunk),
            "metadata": {"chunk_strategy": "paragraph"},
        })

    # Add overlap: prepend tail of previous chunk to each chunk
    if len(chunks) > 1:
        for i in range(1, len(chunks)):
            prev_tail = chunks[i - 1]["text"][-CHUNK_OVERLAP_CHARS:]
            chunks[i]["text"] = prev_tail + "\n\n" + chunks[i]["text"]
            chunks[i]["token_count"] = _estimate_tokens(chunks[i]["text"])

    logger.debug(f"Chunked into {len(chunks)} chunks")
    return chunks


# ─────────────────────────────────────────────
#  EMBEDDING (via Ollama nomic-embed-text)
# ─────────────────────────────────────────────

async def get_embedding(text: str) -> Optional[list[float]]:
    """
    Get a vector embedding for a text chunk using Ollama.
    Model: nomic-embed-text (1536-dim, fast, free)
    Returns None on failure — embedding is optional at ingestion time.
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{settings.ollama_base_url}/api/embed",
                json={
                    "model": "nomic-embed-text",
                    "input": text[:2000],  # truncate to avoid token overflow
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                # Ollama returns {"embeddings": [[...]]}
                embeddings = data.get("embeddings") or data.get("embedding")
                if embeddings:
                    emb = embeddings[0] if isinstance(embeddings[0], list) else embeddings
                    return emb
    except Exception as e:
        logger.warning(f"Embedding failed (non-fatal): {e}")
    return None


# ─────────────────────────────────────────────
#  DATABASE WRITES
# ─────────────────────────────────────────────

async def write_document_to_db(
    db: AsyncSession,
    *,
    document_id: str,
    tenant_id: str,
    user_id: str,
    source_type: str,
    filename: str,
    mime_type: str,
    storage_uri: str,
    checksum: str,
) -> None:
    await db.execute(
        text("""
            INSERT INTO documents
                (document_id, tenant_id, user_id, source_type, filename,
                 mime_type, storage_uri, checksum, status)
            VALUES
                (:doc_id, :tenant_id, :user_id, :source_type, :filename,
                 :mime_type, :storage_uri, :checksum, 'ingested')
            ON CONFLICT (document_id) DO UPDATE
                SET status = 'ingested', updated_at = NOW()
        """),
        {
            "doc_id":      document_id,
            "tenant_id":   tenant_id,
            "user_id":     user_id,
            "source_type": source_type,
            "filename":    filename,
            "mime_type":   mime_type,
            "storage_uri": storage_uri,
            "checksum":    checksum,
        },
    )


async def write_chunks_to_db(
    db: AsyncSession,
    *,
    document_id: str,
    chunks: list[dict],
    embeddings: list[Optional[list[float]]],
) -> list[str]:
    """
    Bulk-insert all chunks. Returns list of chunk_ids.
    """
    import json

    chunk_ids = []
    for chunk, embedding in zip(chunks, embeddings):
        chunk_id = str(uuid.uuid4())
        chunk_ids.append(chunk_id)

        # pgvector expects a string like '[0.1, 0.2, ...]' or NULL
        emb_str = None
        if embedding:
            emb_str = "[" + ",".join(f"{v:.6f}" for v in embedding) + "]"

        if emb_str:
            await db.execute(
                text("""
                    INSERT INTO document_chunks
                        (chunk_id, document_id, chunk_index, text,
                         token_count, embedding, metadata_json)
                    VALUES
                        (:chunk_id, :doc_id, :idx, :text,
                         :token_count, CAST(:embedding AS vector), CAST(:metadata AS jsonb))
                """),
                {
                    "chunk_id":    chunk_id,
                    "doc_id":      document_id,
                    "idx":         chunk["index"],
                    "text":        chunk["text"],
                    "token_count": chunk["token_count"],
                    "embedding":   emb_str,
                    "metadata":    json.dumps(chunk.get("metadata", {})),
                },
            )
        else:
            await db.execute(
                text("""
                    INSERT INTO document_chunks
                        (chunk_id, document_id, chunk_index, text,
                         token_count, metadata_json)
                    VALUES
                        (:chunk_id, :doc_id, :idx, :text,
                         :token_count, CAST(:metadata AS jsonb))
                """),
                {
                    "chunk_id":    chunk_id,
                    "doc_id":      document_id,
                    "idx":         chunk["index"],
                    "text":        chunk["text"],
                    "token_count": chunk["token_count"],
                    "metadata":    json.dumps(chunk.get("metadata", {})),
                },
            )

    # Mark document as parsed
    await db.execute(
        text("UPDATE documents SET status = 'parsed' WHERE document_id = :doc_id"),
        {"doc_id": document_id},
    )

    return chunk_ids


# ─────────────────────────────────────────────
#  MAIN INGESTION ORCHESTRATOR
# ─────────────────────────────────────────────

async def ingest_document(
    db: AsyncSession,
    *,
    file_bytes: bytes,
    filename: str,
    mime_type: str,
    tenant_id: str,
    user_id: str,
    source_type: str,
    embed: bool = True,
) -> dict:
    """
    Full ingestion pipeline.
    Returns summary dict with document_id, chunk_count, status.
    """
    document_id = str(uuid.uuid4())

    logger.info(f"Ingesting {filename} ({mime_type}) for user {user_id}")

    # ── Step 1: Upload raw file to MinIO ─────────────────────────
    try:
        storage_uri, checksum = upload_document(
            file_bytes=file_bytes,
            filename=filename,
            mime_type=mime_type,
            tenant_id=tenant_id,
            document_id=document_id,
        )
        logger.info(f"  ✓ Stored at {storage_uri}")
    except Exception as e:
        logger.error(f"  ✗ MinIO upload failed: {e}")
        # Store without MinIO uri — still continue so we can extract
        storage_uri = f"local/{tenant_id}/{document_id}/{filename}"
        checksum = ""

    # ── Step 2: Write Document row ────────────────────────────────
    await write_document_to_db(
        db,
        document_id=document_id,
        tenant_id=tenant_id,
        user_id=user_id,
        source_type=source_type,
        filename=filename,
        mime_type=mime_type,
        storage_uri=storage_uri,
        checksum=checksum,
    )

    # ── Step 3: Extract text ──────────────────────────────────────
    raw_text = extract_text(file_bytes, mime_type)
    logger.info(f"  ✓ Extracted {len(raw_text):,} characters")

    if not raw_text.strip():
        await db.execute(
            text("UPDATE documents SET status = 'failed' WHERE document_id = :doc_id"),
            {"doc_id": document_id},
        )
        return {
            "document_id":  document_id,
            "status":       "failed",
            "error":        "No text could be extracted from document",
            "chunk_count":  0,
            "storage_uri":  storage_uri,
        }

    # ── Step 4: Chunk ─────────────────────────────────────────────
    chunks = chunk_text(raw_text)
    logger.info(f"  ✓ Chunked into {len(chunks)} segments")

    # ── Step 5: Embed (async, best-effort) ────────────────────────
    embeddings: list[Optional[list[float]]] = [None] * len(chunks)

    if embed:
        for i, chunk in enumerate(chunks):
            emb = await get_embedding(chunk["text"])
            embeddings[i] = emb
            if emb:
                logger.debug(f"    Embedded chunk {i} ({len(emb)} dims)")
        embedded_count = sum(1 for e in embeddings if e is not None)
        logger.info(f"  ✓ Embedded {embedded_count}/{len(chunks)} chunks")

    # ── Step 6: Write chunks to DB ────────────────────────────────
    chunk_ids = await write_chunks_to_db(
        db,
        document_id=document_id,
        chunks=chunks,
        embeddings=embeddings,
    )

    logger.info(f"  ✓ Wrote {len(chunk_ids)} chunks to DB")

    # Audit log (non-fatal — table may not exist in all environments)
    try:
        await db.execute(
            text("""
                INSERT INTO audit_log (tenant_id, actor_user_id, action, object_type, object_id)
                VALUES (:tid, :uid, 'document_ingested', 'document', :doc_id)
            """),
            {"tid": tenant_id, "uid": user_id, "doc_id": document_id},
        )
    except Exception as e:
        logger.warning(f"audit_log write failed (non-fatal): {e}")

    return {
        "document_id":  document_id,
        "filename":     filename,
        "source_type":  source_type,
        "status":       "parsed",
        "chunk_count":  len(chunks),
        "storage_uri":  storage_uri,
        "message":      f"Successfully ingested {len(chunks)} chunks. Ready for extraction.",
    }