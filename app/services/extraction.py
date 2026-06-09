"""
Delllo RAIN3.0 — Extraction Service
"""

from __future__ import annotations

import re
import json
import logging
import uuid
from typing import Optional

import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.config import settings
from app.schemas.ingestion import (
    ExtractionResult,
    ExtractionResponse,
)

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a precise information extraction assistant for a professional network platform.
Your job is to extract structured facts from professional documents (CVs, bios, papers, project descriptions).

RULES:
1. Return ONLY valid JSON. No markdown, no preamble, no explanation.
2. confidence scores must be between 0.0 and 1.0
3. canonical_name must be lowercase snake_case (e.g. "ml_credit_pricing")
4. If a field has no evidence, return an empty array []
5. Be conservative with confidence — only score high if clearly evidenced in the text
6. urgency for objectives and needs: "low" | "medium" | "high"
7. visibility is always "match_engine_only" unless explicitly stated otherwise
8. Extract ALL fact types — especially needs, topics, assets, projects, locations, constraints"""

EXTRACTION_SCHEMA = """{
  "person_id": "<provided_by_caller>",
  "skills": [
    {"name": "Human readable skill name", "canonical_name": "snake_case_name", "confidence": 0.85, "evidence_ref": "brief quote", "visibility": "match_engine_only"}
  ],
  "domains": [
    {"name": "Domain name", "canonical_name": "snake_case_domain", "confidence": 0.90, "evidence_ref": "brief hint"}
  ],
  "topics": [
    {"name": "Topic or subject area", "canonical_name": "snake_case_topic", "confidence": 0.75}
  ],
  "needs": [
    {"text": "Something the person needs help with", "urgency": "high", "confidence": 0.85}
  ],
  "objectives": [
    {"text": "What the person is actively seeking", "urgency": "medium", "visibility": "match_engine_only"}
  ],
  "offers": [
    {"text": "What the person can help others with", "confidence": 0.80}
  ],
  "achievements": [
    {"text": "Specific past achievement with measurable impact", "confidence": 0.75, "evidence_ref": "location"}
  ],
  "assets": [
    {"name": "Title of publication, tool, model, or dataset", "asset_type": "publication", "description": "one line description", "url_or_ref": "journal or URL if mentioned", "confidence": 0.80}
  ],
  "projects": [
    {"name": "Project or initiative name", "description": "brief description", "role": "person role", "organisation": "org name", "confidence": 0.75}
  ],
  "locations": [
    {"site": "Office or building name", "floor": "7", "city": "city if mentioned", "confidence": 0.90}
  ],
  "constraints": [
    {"text": "Availability or collaboration restriction", "constraint_type": "availability", "confidence": 0.85}
  ],
  "privacy_labels": []
}"""


def build_extraction_prompt(chunks_text: str, person_id: str, source_type: str) -> str:
    source_hint = {
        "cv":           "This is a CV / resume.",
        "bio":          "This is a professional biography.",
        "paper":        "This is a research paper or technical document.",
        "note":         "This is a profile note or internal description.",
        "meeting_note": "This is meeting notes.",
        "chat":         "This is a chat/onboarding conversation.",
        "upload":       "This is an uploaded professional document.",
    }.get(source_type, "This is a professional document.")

    return f"""{source_hint}

Extract ALL professional facts from the text below.
Pay special attention to:
- NEEDS: anything the person says they need help with
- ASSETS: publications, papers, tools, models, or datasets they produced
- PROJECTS: named projects or initiatives they worked on
- LOCATIONS: office, city, building, or floor mentions (floor must be a string e.g. "7")
- CONSTRAINTS: availability restrictions, time limits, or collaboration restrictions
- TOPICS: technical or domain topics they engage with

Return JSON matching this schema exactly:

{EXTRACTION_SCHEMA}

Use person_id = "{person_id}"

TEXT TO EXTRACT FROM:
─────────────────────
{chunks_text}
─────────────────────

Return ONLY the JSON object. Start with {{ and end with }}."""


async def call_ollama(prompt: str, system: str = SYSTEM_PROMPT) -> tuple[str, str]:
    payload = {
        "model": settings.ollama_model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": prompt},
        ],
        "stream": False,
        "options": {"temperature": 0.1, "top_p": 0.9, "num_predict": 4096},
        "format": "json",
    }
    async with httpx.AsyncClient(timeout=600.0) as client:
        resp = await client.post(f"{settings.ollama_base_url}/api/chat", json=payload)
        resp.raise_for_status()
        data = resp.json()
    return data["message"]["content"], data.get("model", settings.ollama_model)


def _clean_json_response(raw: str) -> str:
    raw = re.sub(r"```json\s*", "", raw)
    raw = re.sub(r"```\s*",     "", raw)
    raw = raw.strip()
    start, end = raw.find("{"), raw.rfind("}")
    if start != -1 and end != -1:
        return raw[start: end + 1]
    return raw


def parse_extraction_result(raw_response: str, person_id: str) -> ExtractionResult:
    cleaned = _clean_json_response(raw_response)
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse failed: {e}\nRaw: {raw_response[:500]}")
        return ExtractionResult(person_id=person_id, raw_response=raw_response)
    data["person_id"] = person_id
    try:
        result = ExtractionResult(**data)
        result.raw_response = raw_response
        return result
    except Exception as e:
        logger.error(f"Pydantic validation failed: {e}")
        return ExtractionResult(person_id=person_id, raw_response=raw_response)


def _to_canonical(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9\s_]", "", s)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_") or "unknown"


async def write_facts_to_db(
    db: AsyncSession,
    *,
    result: ExtractionResult,
    tenant_id: str,
    user_id: str,
    document_id: str,
    model_used: str,
) -> tuple[int, list[str]]:
    written = 0
    write_errors: list[str] = []

    # ── Delete stale facts for this document before re-inserting ──
    # Prevents accumulation when the same document is re-extracted.
    try:
        await db.execute(
            text("DELETE FROM extracted_facts WHERE source_document_id = :doc_id"),
            {"doc_id": document_id},
        )
    except Exception as e:
        write_errors.append(f"Stale fact cleanup failed: {type(e).__name__}: {e}")

    async def _insert(fact_type, canonical_value, raw_value, confidence, visibility,
                      evidence_ref=None):
        nonlocal written
        chunk_id = None
        if evidence_ref:
            try:
                res = await db.execute(
                    text("SELECT chunk_id FROM document_chunks "
                         "WHERE document_id = :d AND text ILIKE :p LIMIT 1"),
                    {"d": document_id, "p": f"%{evidence_ref[:50]}%"},
                )
                row = res.first()
                if row:
                    chunk_id = str(row[0])
            except Exception:
                pass
        try:
            await db.execute(
                text("""
                    INSERT INTO extracted_facts
                        (fact_id, tenant_id, user_id, fact_type, canonical_value,
                         raw_value, confidence, source_document_id, source_chunk_id,
                         visibility, freshness_date)
                    VALUES
                        (:fid, :tid, :uid, :ft, :canonical,
                         :raw, :conf, :doc_id, :chunk_id, :visibility, NOW()::date)
                    ON CONFLICT DO NOTHING
                """),
                {
                    "fid":       str(uuid.uuid4()),
                    "tid":       tenant_id,
                    "uid":       user_id,
                    "ft":        fact_type,
                    "canonical": canonical_value[:255],
                    "raw":       raw_value[:500],
                    "conf":      round(float(confidence), 3),
                    "doc_id":    document_id,
                    "chunk_id":  chunk_id,
                    "visibility": visibility,
                },
            )
            written += 1
        except Exception as e:
            err = (f"Insert failed ({fact_type} '{canonical_value[:40]}'): "
                   f"{type(e).__name__}: {e}")
            logger.error(err)
            write_errors.append(err)

    try:
        for s in result.skills:
            await _insert("skill", _to_canonical(s.canonical_name or s.name),
                          s.name, s.confidence, s.visibility, s.evidence_ref)
        for d in result.domains:
            await _insert("domain", _to_canonical(d.canonical_name or d.name),
                          d.name, d.confidence, d.visibility, d.evidence_ref)
        for o in result.objectives:
            await _insert("objective", _to_canonical(o.text[:80]),
                          o.text, 0.80, o.visibility)
        for of in result.offers:
            await _insert("offer", _to_canonical(of.text[:80]),
                          of.text, of.confidence, of.visibility)
        for a in result.achievements:
            await _insert("achievement", _to_canonical(a.text[:80]),
                          a.text, a.confidence, a.visibility, a.evidence_ref)
        for t in result.topics:
            await _insert("topic", _to_canonical(t.canonical_name or t.name),
                          t.name, t.confidence, t.visibility)
        for n in result.needs:
            await _insert("need", _to_canonical(n.text[:80]),
                          n.text, n.confidence, n.visibility)
        for asset in result.assets:
            await _insert("asset", _to_canonical(asset.name),
                          asset.name, asset.confidence, asset.visibility,
                          asset.url_or_ref)
        for p in result.projects:
            await _insert("project", _to_canonical(p.name),
                          p.name, p.confidence, p.visibility)
        for loc in result.locations:
            await _insert("location", _to_canonical(loc.site),
                          loc.site, loc.confidence, loc.visibility)
        for c in result.constraints:
            await _insert("constraint", _to_canonical(c.text[:80]),
                          c.text, c.confidence, c.visibility)

        await db.execute(
            text("UPDATE documents SET status = 'extracted' WHERE document_id = :d"),
            {"d": document_id},
        )
        await db.execute(
            text("""
                INSERT INTO audit_log
                    (tenant_id, actor_user_id, action, object_type, object_id, decision_json)
                VALUES
                    (:tid, :uid, 'facts_extracted', 'document', :doc_id, CAST(:meta AS JSONB))
            """),
            {
                "tid":    tenant_id,
                "uid":    user_id,
                "doc_id": document_id,
                "meta":   json.dumps({
                    "model": model_used, "facts_written": written,
                    "skills": len(result.skills), "domains": len(result.domains),
                    "topics": len(result.topics), "needs": len(result.needs),
                    "assets": len(result.assets), "projects": len(result.projects),
                    "locations": len(result.locations),
                    "constraints": len(result.constraints),
                }),
            },
        )
    except Exception as e:
        err = f"write_facts outer error: {type(e).__name__}: {e}"
        logger.error(err)
        write_errors.append(err)

    return written, write_errors


async def extract_from_document(
    db: AsyncSession,
    *,
    document_id: str,
    user_id: str,
    tenant_id: str,
    source_type: str = "cv",
    force_reextract: bool = False,
) -> ExtractionResponse:

    try:
        result = await db.execute(
            text("SELECT status, filename FROM documents "
                 "WHERE document_id = :doc_id AND tenant_id = :tid"),
            {"doc_id": document_id, "tid": tenant_id},
        )
        doc_row = result.mappings().first()
    except Exception as e:
        return ExtractionResponse(
            document_id=uuid.UUID(document_id), user_id=uuid.UUID(user_id),
            facts_written=0, model_used="none", status="error",
            errors=[f"DB error: {type(e).__name__}: {e}"],
        )

    if not doc_row:
        r2 = await db.execute(
            text("SELECT tenant_id FROM documents WHERE document_id = :d"),
            {"d": document_id},
        )
        row2 = r2.mappings().first()
        hint = (f" Exists under tenant={row2['tenant_id']}" if row2
                else " Document does not exist.")
        return ExtractionResponse(
            document_id=uuid.UUID(document_id), user_id=uuid.UUID(user_id),
            facts_written=0, model_used="none", status="error",
            errors=[f"Document not found for tenant={tenant_id}.{hint}"],
        )

    if doc_row["status"] not in ("parsed", "extracted") and not force_reextract:
        return ExtractionResponse(
            document_id=uuid.UUID(document_id), user_id=uuid.UUID(user_id),
            facts_written=0, model_used="none", status="error",
            errors=[f"Status is '{doc_row['status']}', must be 'parsed'. "
                    "Set force_reextract=true to override."],
        )

    chunks_result = await db.execute(
        text("SELECT chunk_index, text, token_count FROM document_chunks "
             "WHERE document_id = :d ORDER BY chunk_index ASC"),
        {"d": document_id},
    )
    chunks = chunks_result.mappings().all()

    if not chunks:
        return ExtractionResponse(
            document_id=uuid.UUID(document_id), user_id=uuid.UUID(user_id),
            facts_written=0, model_used="none", status="error",
            errors=["No chunks found. Run ingestion first."],
        )

    MAX_EXTRACTION_CHARS = 12000   # was 6000 — doubled to avoid silently dropping publications, constraints, location
    combined_text = ""
    truncated = False
    for chunk in chunks:
        if len(combined_text) + len(chunk["text"]) > MAX_EXTRACTION_CHARS:
            truncated = True
            break
        combined_text += chunk["text"] + "\n\n"

    if truncated:
        logger.warning(
            f"Extraction truncated at {MAX_EXTRACTION_CHARS} chars for "
            f"doc={document_id} user={user_id} — later chunks (publications, "
            f"constraints, location) may be missing."
        )

    logger.info(f"Extracting doc={document_id} user={user_id} "
                f"tenant={tenant_id} chars={len(combined_text):,}")

    prompt = build_extraction_prompt(
        chunks_text=combined_text, person_id=user_id, source_type=source_type
    )

    try:
        raw_response, model_used = await call_ollama(prompt)
        logger.warning(f"\n{'='*60}\nRAW OLLAMA ({len(raw_response)} chars):\n"
                       f"{raw_response[:2000]}\n{'='*60}")
    except httpx.ConnectError:
        return ExtractionResponse(
            document_id=uuid.UUID(document_id), user_id=uuid.UUID(user_id),
            facts_written=0, model_used=settings.ollama_model, status="error",
            errors=[f"Cannot connect to Ollama at {settings.ollama_base_url}. "
                    "Run: ollama serve"],
        )
    except Exception as e:
        return ExtractionResponse(
            document_id=uuid.UUID(document_id), user_id=uuid.UUID(user_id),
            facts_written=0, model_used=settings.ollama_model, status="error",
            errors=[f"Ollama error: {type(e).__name__}: {e}"],
        )

    extraction = parse_extraction_result(raw_response, person_id=user_id)
    extraction.model_used = model_used

    logger.info(
        f"Parsed: skills={len(extraction.skills)} domains={len(extraction.domains)} "
        f"topics={len(extraction.topics)} needs={len(extraction.needs)} "
        f"objectives={len(extraction.objectives)} offers={len(extraction.offers)} "
        f"achievements={len(extraction.achievements)} assets={len(extraction.assets)} "
        f"projects={len(extraction.projects)} locations={len(extraction.locations)} "
        f"constraints={len(extraction.constraints)}"
    )

    facts_written, write_errors = await write_facts_to_db(
        db, result=extraction, tenant_id=tenant_id, user_id=user_id,
        document_id=document_id, model_used=model_used,
    )
    logger.info(f"Postgres: {facts_written} facts, {len(write_errors)} errors")

    ikg_errors: list[str] = []
    try:
        from app.db.graph import get_driver
        from app.services import graph_writer

        r = await db.execute(
            text("""
                SELECT u.display_name, p.headline
                FROM users u
                LEFT JOIN user_profiles p ON p.user_id = u.user_id
                WHERE u.user_id = :uid
            """),
            {"uid": user_id},
        )
        profile = r.mappings().first()
        display_name = profile["display_name"] if profile else f"user_{user_id[:8]}"
        headline     = (profile["headline"] or "") if profile else ""

        _, ikg_errors = await graph_writer.write_extraction_to_ikg(
            get_driver(),
            result=extraction,
            person_id=user_id,
            tenant_id=tenant_id,
            display_name=display_name,
            headline=headline,
            document_id=document_id,
        )
    except Exception as e:
        err = f"iKG write outer error: {type(e).__name__}: {e}"
        logger.error(err)
        ikg_errors.append(err)

    if facts_written == 0 and write_errors:
        status = "error"
    elif write_errors:
        status = "partial"
    else:
        status = "completed"

    return ExtractionResponse(
        document_id=uuid.UUID(document_id),
        user_id=uuid.UUID(user_id),
        facts_written=facts_written,
        skills_found=len(extraction.skills),
        domains_found=len(extraction.domains),
        objectives_found=len(extraction.objectives),
        offers_found=len(extraction.offers),
        achievements_found=len(extraction.achievements),
        topics_found=len(extraction.topics),
        needs_found=len(extraction.needs),
        assets_found=len(extraction.assets),
        projects_found=len(extraction.projects),
        locations_found=len(extraction.locations),
        constraints_found=len(extraction.constraints),
        model_used=model_used,
        status=status,
        errors=write_errors,
        ikg_errors=ikg_errors,
    )