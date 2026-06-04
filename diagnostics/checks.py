"""
Delllo RAIN3.0 — Matchmaking Diagnostic Checks

Six checks that go beyond infrastructure health and catch real
matchmaking failures — the ones where every service shows green
but matches silently return zero or garbage results.

Checks:
  1. check_embedding_coverage   — % of chunks with non-null embeddings
  2. check_gkg_seeding          — Memgraph has ProblemType→TxType paths
  3. check_fact_coverage        — users have meaningful extracted facts
  4. check_retrieval_sanity     — a live retrieval call returns candidates
  5. check_score_sanity         — match scores aren't stuck at floor
  6. check_infra_latency        — existing infra checks, with timing
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.config import settings
from app.db.graph import get_driver
from app.services.retrieval import (
    semantic_candidate_search,
    graph_expand_candidates,
    hard_filter,
)
from .models import (
    DiagnosticStatus,
    DiagnosticCheckResult,
    EmbeddingCoverageResult,
    GKGSeedingResult,
    FactCoverageResult,
    RetrievalSanityResult,
    ScoreSanityResult,
)

logger = logging.getLogger(__name__)

# Known transaction types to probe in gKG
KNOWN_TX_TYPES = [
    "technical_problem_solving",
    "knowledge_sharing",
    "collaboration",
    "mentorship",
    "hiring",
]


# ─────────────────────────────────────────────────────────────
#  1. Embedding coverage
# ─────────────────────────────────────────────────────────────

async def check_embedding_coverage(
    db: AsyncSession,
    tenant_id: str,
) -> EmbeddingCoverageResult:
    """
    Checks what % of document_chunks have a non-null embedding vector.
    If this is 0%, semantic search is completely dead even though
    Ollama health check passes.
    """
    try:
        result = await db.execute(
            text("""
                SELECT
                    COUNT(*)                                        AS total_chunks,
                    COUNT(*) FILTER (WHERE dc.embedding IS NOT NULL) AS embedded_chunks,
                    d.user_id
                FROM document_chunks dc
                JOIN documents d ON d.document_id = dc.document_id
                WHERE d.tenant_id = :tid
                GROUP BY d.user_id
            """),
            {"tid": tenant_id},
        )
        rows = result.mappings().all()

        total     = sum(r["total_chunks"] for r in rows)
        embedded  = sum(r["embedded_chunks"] for r in rows)
        zero_emb  = [str(r["user_id"]) for r in rows if r["embedded_chunks"] == 0]

        pct = round((embedded / total * 100) if total > 0 else 0.0, 1)

        if total == 0:
            status = DiagnosticStatus.WARN
            detail = "No document chunks found — no documents have been ingested yet"
        elif pct == 0:
            status = DiagnosticStatus.ERROR
            detail = (
                f"0% embedding coverage: {total} chunks exist but NONE have embeddings. "
                f"Semantic search is completely disabled. "
                f"Re-ingest documents with embed=True or run embedding backfill."
            )
        elif pct < 50:
            status = DiagnosticStatus.WARN
            detail = (
                f"Low embedding coverage: {pct}% ({embedded}/{total} chunks). "
                f"Semantic search will miss many candidates."
            )
        else:
            status = DiagnosticStatus.OK
            detail = f"Embedding coverage: {pct}% ({embedded}/{total} chunks)"

        return EmbeddingCoverageResult(
            total_chunks=total,
            embedded_chunks=embedded,
            coverage_pct=pct,
            users_with_zero_embeddings=zero_emb,
            status=status,
            detail=detail,
        )

    except Exception as e:
        logger.error(f"check_embedding_coverage failed: {e}")
        return EmbeddingCoverageResult(
            total_chunks=0,
            embedded_chunks=0,
            coverage_pct=0.0,
            users_with_zero_embeddings=[],
            status=DiagnosticStatus.ERROR,
            detail=f"Check failed: {e}",
        )


# ─────────────────────────────────────────────────────────────
#  2. gKG seeding
# ─────────────────────────────────────────────────────────────

async def check_gkg_seeding() -> GKGSeedingResult:
    """
    Verifies that Memgraph contains ProblemType→TransactionType→CapabilityType
    chains for the known transaction types.

    If this is empty, graph_expand_candidates() will always return 0 results
    and fall through to the weak Postgres ILIKE fallback — silently.
    """
    driver = get_driver()
    seeded: list[str] = []
    empty:  list[str] = []
    total_nodes = 0
    total_edges = 0

    try:
        async with driver.session() as session:
            # Overall node/edge counts
            node_result = await session.run("MATCH (n) RETURN COUNT(n) AS count")
            node_data = await node_result.single()
            total_nodes = node_data["count"] if node_data else 0

            edge_result = await session.run("MATCH ()-[r]->() RETURN COUNT(r) AS count")
            edge_data = await edge_result.single()
            total_edges = edge_data["count"] if edge_data else 0

            # Check each known tx type for a traversable path
            for tx_type in KNOWN_TX_TYPES:
                r = await session.run(
                    """
                    MATCH (pt:ProblemType)-[:MAPS_TO]->
                          (tt:TransactionType {type_id: $type_id})
                    MATCH (pt)-[:REQUIRES]->(cap:CapabilityType)
                    RETURN COUNT(cap) AS cap_count
                    """,
                    {"type_id": f"tt_{tx_type}"},
                )
                data = await r.single()
                cap_count = data["cap_count"] if data else 0
                if cap_count > 0:
                    seeded.append(tx_type)
                else:
                    empty.append(tx_type)

        if total_nodes == 0:
            status = DiagnosticStatus.ERROR
            detail = (
                "Memgraph is empty — no nodes exist. "
                "The gKG has not been seeded. "
                "Graph expansion will always return 0 candidates. "
                "Run your graph seeding script to populate ProblemType/TransactionType chains."
            )
        elif empty and not seeded:
            status = DiagnosticStatus.ERROR
            detail = (
                f"gKG has {total_nodes} nodes but no traversable tx_type paths. "
                f"All {len(empty)} transaction types return 0 capabilities. "
                f"Check ProblemType→TransactionType→CapabilityType seeding."
            )
        elif empty:
            status = DiagnosticStatus.WARN
            detail = (
                f"{len(seeded)}/{len(KNOWN_TX_TYPES)} transaction types have gKG paths. "
                f"Missing: {', '.join(empty)}"
            )
        else:
            status = DiagnosticStatus.OK
            detail = (
                f"All {len(seeded)} transaction types have gKG paths. "
                f"Graph: {total_nodes} nodes, {total_edges} edges."
            )

        return GKGSeedingResult(
            transaction_types_checked=KNOWN_TX_TYPES,
            seeded_types=seeded,
            empty_types=empty,
            total_nodes=total_nodes,
            total_edges=total_edges,
            status=status,
            detail=detail,
        )

    except Exception as e:
        logger.error(f"check_gkg_seeding failed: {e}")
        return GKGSeedingResult(
            transaction_types_checked=KNOWN_TX_TYPES,
            seeded_types=[],
            empty_types=KNOWN_TX_TYPES,
            total_nodes=0,
            total_edges=0,
            status=DiagnosticStatus.ERROR,
            detail=f"Memgraph query failed: {e}",
        )


# ─────────────────────────────────────────────────────────────
#  3. Fact coverage
# ─────────────────────────────────────────────────────────────

async def check_fact_coverage(
    db: AsyncSession,
    tenant_id: str,
) -> FactCoverageResult:
    """
    Checks that users have meaningful, retrievable extracted facts.

    Catches:
    - Cold-start users (zero facts) → invisible to matching
    - Users with only match_engine_only facts → invisible to ILIKE fallback
      (they pass the retrieval filter but produce weak matches)
    - Low average confidence across all facts
    """
    try:
        result = await db.execute(
            text("""
                SELECT
                    u.user_id,
                    COUNT(ef.fact_id)                                       AS total_facts,
                    COUNT(ef.fact_id) FILTER (WHERE ef.visibility != 'private') AS visible_facts,
                    COUNT(ef.fact_id) FILTER (
                        WHERE ef.visibility = 'match_engine_only'
                    )                                                       AS engine_only_facts,
                    COALESCE(AVG(ef.confidence), 0)                        AS avg_confidence
                FROM users u
                JOIN user_tenants ut ON ut.user_id = u.user_id
                LEFT JOIN extracted_facts ef
                    ON ef.user_id = u.user_id AND ef.tenant_id = :tid
                WHERE ut.tenant_id = :tid AND ut.status = 'active'
                GROUP BY u.user_id
            """),
            {"tid": tenant_id},
        )
        rows = result.mappings().all()

        total_users          = len(rows)
        cold_start           = sum(1 for r in rows if r["total_facts"] == 0)
        only_engine_visible  = sum(
            1 for r in rows
            if r["total_facts"] > 0 and r["total_facts"] == r["engine_only_facts"]
        )
        low_conf             = sum(1 for r in rows if float(r["avg_confidence"]) < 0.5
                                   and r["total_facts"] > 0)
        avg_facts            = (
            sum(r["total_facts"] for r in rows) / total_users
            if total_users > 0 else 0.0
        )

        cold_pct = round(cold_start / total_users * 100, 1) if total_users > 0 else 0

        if cold_start == total_users:
            status = DiagnosticStatus.ERROR
            detail = (
                f"ALL {total_users} users have zero extracted facts. "
                f"The extraction pipeline has not run. "
                f"Call POST /v1/ingest/pipeline for each user."
            )
        elif cold_pct > 50:
            status = DiagnosticStatus.ERROR
            detail = (
                f"{cold_start}/{total_users} users ({cold_pct}%) have no facts. "
                f"Matchmaking will silently skip most users."
            )
        elif cold_pct > 20 or only_engine_visible > (total_users * 0.3):
            status = DiagnosticStatus.WARN
            detail = (
                f"{cold_start} cold-start users, "
                f"{only_engine_visible} users with only engine-visible facts "
                f"(invisible to ILIKE fallback retrieval). "
                f"Average facts per user: {avg_facts:.1f}."
            )
        else:
            status = DiagnosticStatus.OK
            detail = (
                f"Fact coverage healthy: {total_users - cold_start}/{total_users} users "
                f"have facts. Avg {avg_facts:.1f} facts/user."
            )

        return FactCoverageResult(
            total_users=total_users,
            users_with_facts=total_users - cold_start,
            cold_start_users=cold_start,
            users_only_engine_visible=only_engine_visible,
            avg_facts_per_user=round(avg_facts, 2),
            low_confidence_users=low_conf,
            status=status,
            detail=detail,
        )

    except Exception as e:
        logger.error(f"check_fact_coverage failed: {e}")
        return FactCoverageResult(
            total_users=0,
            users_with_facts=0,
            cold_start_users=0,
            users_only_engine_visible=0,
            avg_facts_per_user=0.0,
            low_confidence_users=0,
            status=DiagnosticStatus.ERROR,
            detail=f"Check failed: {e}",
        )


# ─────────────────────────────────────────────────────────────
#  4. Retrieval sanity
# ─────────────────────────────────────────────────────────────

async def check_retrieval_sanity(
    db: AsyncSession,
    tenant_id: str,
    transaction_type: str = "technical_problem_solving",
) -> RetrievalSanityResult:
    """
    Runs a live retrieval call using the first active user in the tenant
    as a test requester. Asserts that at least some candidates come back
    from each retrieval path (semantic and/or graph).

    This is the only check that will catch:
    - ILIKE token splitting returning 0 results
    - Embeddings present but vector distance query failing
    - gKG paths existing but Person nodes not being linked
    """
    # Pick a test user: first active user with at least some facts
    try:
        test_user_result = await db.execute(
            text("""
                SELECT DISTINCT u.user_id
                FROM users u
                JOIN user_tenants ut ON ut.user_id = u.user_id
                JOIN extracted_facts ef
                    ON ef.user_id = u.user_id AND ef.tenant_id = :tid
                WHERE ut.tenant_id = :tid AND ut.status = 'active'
                LIMIT 1
            """),
            {"tid": tenant_id},
        )
        row = test_user_result.mappings().first()

        if not row:
            return RetrievalSanityResult(
                test_user_id="none",
                transaction_type=transaction_type,
                semantic_hits=0,
                graph_hits=0,
                merged_candidates=0,
                after_hard_filter=0,
                used_fallback=False,
                status=DiagnosticStatus.WARN,
                detail="No active users with facts found — cannot run retrieval sanity check.",
            )

        test_user_id = str(row["user_id"])

        # Build a realistic query text from the user's own facts
        facts_result = await db.execute(
            text("""
                SELECT canonical_value FROM extracted_facts
                WHERE user_id = :uid AND tenant_id = :tid
                  AND fact_type IN ('skill', 'domain', 'need', 'objective')
                  AND visibility != 'private'
                LIMIT 5
            """),
            {"uid": test_user_id, "tid": tenant_id},
        )
        fact_values  = [r["canonical_value"] for r in facts_result.mappings().all()]
        query_text   = " ".join(fact_values) if fact_values else transaction_type.replace("_", " ")

        # Run each retrieval path independently so we can report on each
        semantic_ids = await semantic_candidate_search(
            db,
            tenant_id=tenant_id,
            requester_id=test_user_id,
            query_text=query_text,
            top_k=20,
        )

        graph_ids = await graph_expand_candidates(
            db,
            tenant_id=tenant_id,
            requester_id=test_user_id,
            transaction_type=transaction_type,
            top_k=20,
        )

        merged = list({*semantic_ids, *graph_ids})

        filtered = await hard_filter(
            db,
            tenant_id=tenant_id,
            requester_id=test_user_id,
            candidate_ids=merged,
        )

        used_fallback = len(semantic_ids) == 0 and len(graph_ids) == 0

        if len(filtered) == 0 and not used_fallback:
            status = DiagnosticStatus.ERROR
            detail = (
                f"Retrieval returned 0 candidates after hard filter "
                f"(semantic={len(semantic_ids)}, graph={len(graph_ids)}, "
                f"merged={len(merged)}). "
                f"Hard filter may be over-excluding — check for missing active users or facts."
            )
        elif used_fallback and len(filtered) == 0:
            status = DiagnosticStatus.ERROR
            detail = (
                "Both semantic and graph retrieval returned 0. "
                "The fallback pool was also empty. "
                "Check: embedding coverage, gKG seeding, and fact extraction."
            )
        elif used_fallback:
            status = DiagnosticStatus.WARN
            detail = (
                f"Both semantic and graph retrieval returned 0 — relying on basic fallback pool. "
                f"Fallback returned {len(filtered)} candidates. "
                f"Fix embeddings or gKG seeding for proper retrieval."
            )
        elif len(semantic_ids) == 0:
            status = DiagnosticStatus.WARN
            detail = (
                f"Semantic search returned 0 candidates (embeddings may be missing). "
                f"Graph returned {len(graph_ids)} candidates. "
                f"Final pool: {len(filtered)} after filter."
            )
        elif len(graph_ids) == 0:
            status = DiagnosticStatus.WARN
            detail = (
                f"Graph expansion returned 0 candidates (gKG may be unseeded). "
                f"Semantic returned {len(semantic_ids)} candidates. "
                f"Final pool: {len(filtered)} after filter."
            )
        else:
            status = DiagnosticStatus.OK
            detail = (
                f"Retrieval healthy: semantic={len(semantic_ids)}, "
                f"graph={len(graph_ids)}, final={len(filtered)}."
            )

        return RetrievalSanityResult(
            test_user_id=test_user_id,
            transaction_type=transaction_type,
            semantic_hits=len(semantic_ids),
            graph_hits=len(graph_ids),
            merged_candidates=len(merged),
            after_hard_filter=len(filtered),
            used_fallback=used_fallback,
            status=status,
            detail=detail,
        )

    except Exception as e:
        logger.error(f"check_retrieval_sanity failed: {e}")
        return RetrievalSanityResult(
            test_user_id="error",
            transaction_type=transaction_type,
            semantic_hits=0,
            graph_hits=0,
            merged_candidates=0,
            after_hard_filter=0,
            used_fallback=False,
            status=DiagnosticStatus.ERROR,
            detail=f"Check failed: {e}",
        )


# ─────────────────────────────────────────────────────────────
#  5. Score sanity
# ─────────────────────────────────────────────────────────────

async def check_score_sanity(
    db: AsyncSession,
    tenant_id: str,
    sample_size: int = 100,
) -> ScoreSanityResult:
    """
    Inspects recently-generated match scores for the tenant and flags
    two failure modes:

    a) Floor clustering — all scores near 0.0 means scoring features
       are all zero (usually because profiles have no facts or signals).

    b) Dead feature columns — a feature that is 0 for every single match
       means that dimension is not contributing to any score. This often
       means a data path (e.g. offer/need extraction) is broken.
    """
    try:
        result = await db.execute(
            text("""
                SELECT
                    m.score,
                    ms.relevance, ms.complementarity, ms.timing,
                    ms.proximity, ms.evidence_strength, ms.outcome_likelihood,
                    ms.novelty, ms.privacy_risk, ms.interaction_friction
                FROM matches m
                JOIN match_scores ms ON ms.match_id = m.match_id
                WHERE m.tenant_id = :tid
                ORDER BY m.created_at DESC
                LIMIT :n
            """),
            {"tid": tenant_id, "n": sample_size},
        )
        rows = result.mappings().all()

        n = len(rows)
        if n == 0:
            return ScoreSanityResult(
                sample_size=0,
                min_score=0.0,
                max_score=0.0,
                avg_score=0.0,
                pct_below_0_2=0.0,
                pct_above_0_7=0.0,
                all_zero_features=[],
                status=DiagnosticStatus.WARN,
                detail="No match scores found — matches have not been generated yet.",
            )

        scores     = [float(r["score"]) for r in rows]
        min_s      = min(scores)
        max_s      = max(scores)
        avg_s      = round(sum(scores) / n, 3)
        pct_low    = round(sum(1 for s in scores if s < 0.2) / n * 100, 1)
        pct_high   = round(sum(1 for s in scores if s > 0.7) / n * 100, 1)

        # Identify dead feature columns
        feature_cols = [
            "relevance", "complementarity", "timing", "proximity",
            "evidence_strength", "outcome_likelihood", "novelty",
            "privacy_risk", "interaction_friction",
        ]
        dead_features = [
            col for col in feature_cols
            if all(float(r[col] or 0) == 0.0 for r in rows)
        ]

        if pct_low > 80:
            status = DiagnosticStatus.ERROR
            detail = (
                f"{pct_low}% of matches score below 0.2 — scores are clustering at the floor. "
                f"Most likely cause: users lack needs/objectives so relevance and "
                f"complementarity are always 0. Check extraction quality."
            )
        elif dead_features:
            status = DiagnosticStatus.WARN
            detail = (
                f"These scoring features are 0 across ALL {n} sampled matches: "
                f"{', '.join(dead_features)}. "
                f"The corresponding data paths may be broken."
            )
        elif pct_low > 50:
            status = DiagnosticStatus.WARN
            detail = (
                f"{pct_low}% of matches score below 0.2. "
                f"Scores may be weak due to sparse fact profiles."
            )
        else:
            status = DiagnosticStatus.OK
            detail = (
                f"Score distribution looks healthy: "
                f"avg={avg_s:.3f}, min={min_s:.3f}, max={max_s:.3f}, "
                f"{pct_high}% above 0.7."
            )

        return ScoreSanityResult(
            sample_size=n,
            min_score=round(min_s, 4),
            max_score=round(max_s, 4),
            avg_score=avg_s,
            pct_below_0_2=pct_low,
            pct_above_0_7=pct_high,
            all_zero_features=dead_features,
            status=status,
            detail=detail,
        )

    except Exception as e:
        logger.error(f"check_score_sanity failed: {e}")
        return ScoreSanityResult(
            sample_size=0,
            min_score=0.0, max_score=0.0, avg_score=0.0,
            pct_below_0_2=0.0, pct_above_0_7=0.0,
            all_zero_features=[],
            status=DiagnosticStatus.ERROR,
            detail=f"Check failed: {e}",
        )


# ─────────────────────────────────────────────────────────────
#  6. Infra latency checks (augments health.py with timing)
# ─────────────────────────────────────────────────────────────

async def check_postgres_latency(db: AsyncSession) -> DiagnosticCheckResult:
    t0 = time.perf_counter()
    try:
        result = await db.execute(text("SELECT version()"))
        version = result.scalar()
        latency = round((time.perf_counter() - t0) * 1000, 1)
        status = DiagnosticStatus.WARN if latency > 500 else DiagnosticStatus.OK
        return DiagnosticCheckResult(
            service="postgres",
            status=status,
            detail=version.split(",")[0] if version else "connected",
            latency_ms=latency,
        )
    except Exception as e:
        return DiagnosticCheckResult(
            service="postgres",
            status=DiagnosticStatus.ERROR,
            detail=str(e),
            latency_ms=round((time.perf_counter() - t0) * 1000, 1),
        )


async def check_memgraph_latency() -> DiagnosticCheckResult:
    t0 = time.perf_counter()
    try:
        driver = get_driver()
        async with driver.session() as s:
            result = await s.run("RETURN 1 AS ping")
            await result.single()
        latency = round((time.perf_counter() - t0) * 1000, 1)
        status = DiagnosticStatus.WARN if latency > 1000 else DiagnosticStatus.OK
        return DiagnosticCheckResult(
            service="memgraph",
            status=status,
            detail="Bolt connection healthy",
            latency_ms=latency,
        )
    except Exception as e:
        return DiagnosticCheckResult(
            service="memgraph",
            status=DiagnosticStatus.ERROR,
            detail=str(e),
            latency_ms=round((time.perf_counter() - t0) * 1000, 1),
        )


async def check_ollama_latency() -> DiagnosticCheckResult:
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{settings.ollama_base_url}/api/tags")
            latency = round((time.perf_counter() - t0) * 1000, 1)
            if resp.status_code == 200:
                models = [m["name"] for m in resp.json().get("models", [])]
                embed_model = "nomic-embed-text"
                chat_model  = settings.ollama_model
                missing = [
                   m for m in [embed_model, chat_model]
                   if not any(m == avail or avail.startswith(m + ":") for avail in models)
                ]   
                if missing:
                    return DiagnosticCheckResult(
                        service="ollama",
                        status=DiagnosticStatus.WARN,
                        detail=f"Connected but missing models: {', '.join(missing)}. Run: ollama pull <model>",
                        latency_ms=latency,
                        metadata={"available_models": models},
                    )
                return DiagnosticCheckResult(
                    service="ollama",
                    status=DiagnosticStatus.OK,
                    detail=f"Both models present: {embed_model}, {chat_model}",
                    latency_ms=latency,
                    metadata={"available_models": models},
                )
    except Exception as e:
        pass

    latency = round((time.perf_counter() - t0) * 1000, 1)
    return DiagnosticCheckResult(
        service="ollama",
        status=DiagnosticStatus.ERROR,
        detail=f"Ollama unreachable at {settings.ollama_base_url}",
        latency_ms=latency,
    )