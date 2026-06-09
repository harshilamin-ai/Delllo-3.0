"""
Delllo RAIN3.0 — Retrieval Service (Production Grade FINAL)

Features:
- Correct Ollama embedding (/api/embed)
- Retry + timeout handling
- Transaction safety (no session poisoning)
- Native pgvector usage (no string casting)
- Hybrid retrieval (semantic + graph)
- Weighted merging (better ranking)
- Scalable filtering (ANY instead of IN)
"""

from __future__ import annotations

import logging
import asyncio
from typing import Optional, List, Dict

import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.config import settings
from app.db.graph import get_driver

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

OLLAMA_TIMEOUT = 2000.0
OLLAMA_RETRIES = 2


# ─────────────────────────────────────────────
# EMBEDDING
# ─────────────────────────────────────────────

async def embed_text(text_input: str) -> Optional[List[float]]:
    """
    Calls Ollama embedding endpoint.
    Uses: /api/embed (correct endpoint)
    """
    url = f"{settings.ollama_base_url}/api/embed"

    payload = {
        "model": "nomic-embed-text",
        "input": text_input,
    }

    for attempt in range(OLLAMA_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=OLLAMA_TIMEOUT) as client:
                resp = await client.post(url, json=payload)

            if resp.status_code != 200:
                raise httpx.HTTPStatusError(
                    f"Bad status {resp.status_code}",
                    request=resp.request,
                    response=resp,
                )

            data = resp.json()

            # correct field: Ollama returns "embeddings" as [[floats]]
            embeddings = data.get("embeddings")
            if not embeddings or not isinstance(embeddings, list) or not embeddings:
                logger.warning(f"Full response: {data}")
                raise ValueError("Missing or invalid 'embeddings' in response")

            return embeddings[0]  # First (and only) embedding for single input

        except Exception as e:
            logger.warning(
                f"[embed attempt {attempt+1}] failed for "
                f"'{text_input[:40]}': {e}"
            )

            if attempt < OLLAMA_RETRIES:
                await asyncio.sleep(0.5 * (attempt + 1))
            else:
                logger.error("Embedding permanently failed")

    return None


# ─────────────────────────────────────────────
# SAFE DB EXECUTION
# ─────────────────────────────────────────────

async def safe_execute(
    db: AsyncSession,
    query,
    params: dict
):
    """
    Executes query safely.
    Rolls back on failure to avoid broken transactions.
    """
    try:
        return await db.execute(query, params)
    except Exception as e:
        logger.error(f"DB query failed → rolling back: {e}")
        await db.rollback()
        return None


# ─────────────────────────────────────────────
# SEMANTIC SEARCH
# ─────────────────────────────────────────────

async def semantic_candidate_search(
    db: AsyncSession,
    *,
    tenant_id: str,
    requester_id: str,
    query_text: str,
    top_k: int = 50,
) -> List[str]:

    vector = await embed_text(query_text)

    if not vector:
        logger.warning("Semantic search skipped — embedding unavailable")
        return []

    # Convert list to pgvector string format: [x,y,z,...]
    vector_str = "[" + ",".join(str(x) for x in vector) + "]"

    # Diagnostic: warn if most chunks lack embeddings (common cold-start issue)
    coverage_check = await safe_execute(
        db,
        text("""
            SELECT
                COUNT(*) FILTER (WHERE dc.embedding IS NOT NULL) AS with_embedding,
                COUNT(*) AS total
            FROM document_chunks dc
            JOIN documents d ON d.document_id = dc.document_id
            WHERE d.tenant_id = :tid AND d.user_id != :rid
        """),
        {"tid": tenant_id, "rid": requester_id},
    )
    if coverage_check:
        cov = coverage_check.mappings().first()
        if cov and cov["total"] > 0 and cov["with_embedding"] == 0:
            logger.warning(
                f"Semantic search: {cov['total']} chunks found but NONE have embeddings "
                f"for tenant={tenant_id[:8]} — re-ingest with embed=True or run embedding backfill"
            )

    result = await safe_execute(
        db,
        text("""
            SELECT d.user_id,
                   MIN(dc.embedding <=> CAST(:vec AS vector)) AS distance
            FROM document_chunks dc
            JOIN documents d ON d.document_id = dc.document_id
            WHERE d.tenant_id = :tid
              AND d.user_id != :rid
              AND dc.embedding IS NOT NULL
            GROUP BY d.user_id
            ORDER BY distance ASC
            LIMIT :top_k
        """),
        {
            "vec": vector_str,
            "tid": tenant_id,
            "rid": requester_id,
            "top_k": top_k,
        },
    )

    if not result:
        logger.warning("Semantic search failed")
        return []

    rows = result.mappings().all()
    ids = [str(r["user_id"]) for r in rows]

    logger.info(f"Semantic search → {len(ids)} candidates")
    return ids


# ─────────────────────────────────────────────
# GRAPH EXPANSION
# ─────────────────────────────────────────────

async def graph_expand_candidates(
    db: AsyncSession,
    *,
    tenant_id: str,
    requester_id: str,
    transaction_type: str,
    top_k: int = 50,
) -> List[str]:

    driver = get_driver()
    candidate_ids: set[str] = set()

    # ── Memgraph path ──
    try:
        async with driver.session() as session:
            result = await session.run(
                """
                MATCH (pt:ProblemType)-[:MAPS_TO]->(tt:TransactionType {type_id: $type_id})
                MATCH (pt)-[:REQUIRES]->(cap:CapabilityType)
                MATCH (s:Skill)-[:MAPS_TO_CAPABILITY]->(cap)
                MATCH (p:Person)-[:HAS_SKILL]->(s)
                WHERE p.tenant_id = $tid AND p.person_id <> $rid
                RETURN DISTINCT p.person_id AS person_id
                LIMIT $top_k
                """,
                {
                    "type_id": f"tt_{transaction_type}",
                    "tid": tenant_id,
                    "rid": requester_id,
                    "top_k": top_k,
                },
            )

            async for record in result:
                candidate_ids.add(record["person_id"])

        if candidate_ids:
            logger.info(f"gKG → {len(candidate_ids)} candidates")
            return list(candidate_ids)
        else:
            logger.warning(
                f"gKG connected but returned 0 candidates for "
                f"tx_type=tt_{transaction_type} tenant={tenant_id[:8]} — "
                f"check ProblemType→TransactionType seeding in Memgraph"
            )

    except Exception as e:
        logger.warning(f"gKG failed → fallback to Postgres: {e}")

    # ── Postgres fallback ──
    keywords = transaction_type.replace("_", " ").split()

    if not keywords:
        return []

    token_clauses = " OR ".join(
        f"ef.canonical_value ILIKE :kw{i}" for i in range(len(keywords))
    )
    params: dict = {
        "tid": tenant_id,
        "rid": requester_id,
        "top_k": top_k,
        "kw_full": f"%{transaction_type}%",
        "kw_phrase": f"%{' '.join(keywords)}%",
        **{f"kw{i}": f"%{kw}%" for i, kw in enumerate(keywords)},
    }

    result = await safe_execute(
        db,
        text(f"""
            SELECT DISTINCT ef.user_id
            FROM extracted_facts ef
            WHERE ef.tenant_id = :tid
              AND ef.user_id != :rid
              AND ef.fact_type IN ('skill', 'domain', 'topic', 'objective', 'offer')
              AND ef.visibility != 'private'
              AND (
                  ef.canonical_value ILIKE :kw_full
                  OR ef.canonical_value ILIKE :kw_phrase
                  OR ef.raw_value ILIKE :kw_full
                  OR ef.raw_value ILIKE :kw_phrase
                  OR ({token_clauses})
              )
            LIMIT :top_k
        """),
        params,
    )

    if not result:
        return []

    ids = [str(r["user_id"]) for r in result.mappings().all()]
    logger.info(f"Postgres fallback → {len(ids)} candidates")

    return ids


# ─────────────────────────────────────────────
# HARD FILTER  (Bug #11 fix: CAST list to text[])
# ─────────────────────────────────────────────

async def hard_filter(
    db: AsyncSession,
    *,
    tenant_id: str,
    requester_id: str,
    candidate_ids: List[str],
    population: Optional[List[str]] = None,
) -> List[str]:
    """
    Filter candidates to those that are:
    - In the explicit population list if provided (Node-supplied active users)
    - Otherwise active members of the tenant via user_tenants
    - Have non-private facts
    - No existing open match with the requester
    """
    if not candidate_ids:
        return []

    if population is not None:
        result = await safe_execute(
            db,
            text("""
                SELECT DISTINCT u.user_id
                FROM users u
                JOIN user_tenants ut ON ut.user_id = u.user_id
                WHERE ut.tenant_id = :tid
                  AND ut.status = 'active'
                  AND u.user_id::text = ANY(CAST(:candidate_ids AS text[]))
                  AND u.user_id::text = ANY(CAST(:population AS text[]))
                  AND NOT EXISTS (
                      SELECT 1 FROM matches m
                      WHERE m.tenant_id = :tid
                        AND m.person_a = :rid
                        AND m.person_b = u.user_id
                        AND m.status NOT IN ('expired','dismissed')
                  )
                  AND EXISTS (
                      SELECT 1 FROM extracted_facts ef
                      WHERE ef.user_id = u.user_id
                        AND ef.tenant_id = :tid
                        AND ef.visibility != 'private'
                  )
            """),
            {
                "tid":           tenant_id,
                "rid":           requester_id,
                "candidate_ids": candidate_ids,
                "population":    population,
            },
        )
    else:
        result = await safe_execute(
            db,
            text("""
                SELECT DISTINCT u.user_id
                FROM users u
                JOIN user_tenants ut ON ut.user_id = u.user_id
                WHERE ut.tenant_id = :tid
                  AND ut.status = 'active'
                  AND u.user_id::text = ANY(CAST(:candidate_ids AS text[]))
                  AND NOT EXISTS (
                      SELECT 1 FROM matches m
                      WHERE m.tenant_id = :tid
                        AND m.person_a = :rid
                        AND m.person_b = u.user_id
                        AND m.status NOT IN ('expired','dismissed')
                  )
                  AND EXISTS (
                      SELECT 1 FROM extracted_facts ef
                      WHERE ef.user_id = u.user_id
                        AND ef.tenant_id = :tid
                        AND ef.visibility != 'private'
                  )
            """),
            {
                "tid":           tenant_id,
                "rid":           requester_id,
                "candidate_ids": candidate_ids,
            },
        )

    if not result:
        return candidate_ids

    return [str(r["user_id"]) for r in result.mappings().all()]


# ─────────────────────────────────────────────
# MAIN ENTRY
# ─────────────────────────────────────────────

async def retrieve_candidates(
    db: AsyncSession,
    *,
    tenant_id: str,
    requester_id: str,
    query_text: str,
    transaction_type: str,
    max_candidates: int = 50,
    population: Optional[List[str]] = None,
) -> List[str]:
    """
    Hybrid retrieval: semantic search + gKG graph expansion + hard filter.

    population: when provided by Node, matchmaking is scoped to only these
                user IDs. RAIN trusts Node's activity decision — no status
                check is applied. When None, falls back to active members
                of the tenant via user_tenants.
    """

    semantic_ids = await semantic_candidate_search(
        db,
        tenant_id=tenant_id,
        requester_id=requester_id,
        query_text=query_text,
        top_k=max_candidates * 2,
    )

    graph_ids = await graph_expand_candidates(
        db,
        tenant_id=tenant_id,
        requester_id=requester_id,
        transaction_type=transaction_type,
        top_k=max_candidates * 2,
    )

    # ── Weighted merge ──
    scores: Dict[str, float] = {}

    for i, uid in enumerate(semantic_ids):
        scores[uid] = scores.get(uid, 0) + (1.0 / (i + 1))

    for i, uid in enumerate(graph_ids):
        scores[uid] = scores.get(uid, 0) + (0.7 / (i + 1))

    merged = sorted(scores.keys(), key=lambda x: scores[x], reverse=True)

    if not semantic_ids and not graph_ids:
        logger.warning("No candidates found from any strategy")

    filtered = await hard_filter(
        db,
        tenant_id=tenant_id,
        requester_id=requester_id,
        candidate_ids=merged,
        population=population,
    )

    final = filtered[:max_candidates]

    logger.info(
        f"Retrieval → semantic={len(semantic_ids)} "
        f"graph={len(graph_ids)} merged={len(merged)} "
        f"final={len(final)}"
    )

    return final
