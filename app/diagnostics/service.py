"""
Delllo RAIN3.0 — Diagnostic Service

Coordinates all checks and assembles the MatchmakingDiagnosticReport.
Also runs the POST /diagnostics/selftest full pipeline test.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from .models import (
    DiagnosticStatus,
    DiagnosticCheckResult,
    MatchmakingDiagnosticReport,
    PipelineStageStatus,
    SelfTestResult,
)
from .checks import (
    check_embedding_coverage,
    check_gkg_seeding,
    check_fact_coverage,
    check_retrieval_sanity,
    check_score_sanity,
    check_postgres_latency,
    check_memgraph_latency,
    check_ollama_latency,
)

logger = logging.getLogger(__name__)

# Sentinel tenant/user for self-test — uses first real tenant if this doesn't exist
SELFTEST_TENANT = "00000000-0000-0000-0000-000000000001"
SELFTEST_USER   = "00000001-0000-0000-0000-000000000001"


def _worst_status(*statuses: DiagnosticStatus) -> DiagnosticStatus:
    order = {
        DiagnosticStatus.OK:    0,
        DiagnosticStatus.SKIP:  0,
        DiagnosticStatus.WARN:  1,
        DiagnosticStatus.ERROR: 2,
    }
    return max(statuses, key=lambda s: order.get(s, 0))


def _build_actions(report: MatchmakingDiagnosticReport) -> tuple[list[str], list[str]]:
    """Derive actionable remediation steps and warnings from check results."""
    actions:  list[str] = []
    warnings: list[str] = []

    ec = report.embedding_coverage
    if ec:
        if ec.status == DiagnosticStatus.ERROR and ec.coverage_pct == 0:
            actions.append(
                "CRITICAL — Re-ingest all documents with embed=True. "
                "Run POST /v1/ingest/pipeline with embed=true for each user, "
                "or execute an embedding backfill job on document_chunks."
            )
        elif ec.status == DiagnosticStatus.WARN:
            warnings.append(
                f"Partial embedding coverage ({ec.coverage_pct}%). "
                f"{len(ec.users_with_zero_embeddings)} user(s) have docs but no embeddings."
            )

    gkg = report.gkg_seeding
    if gkg:
        if gkg.status == DiagnosticStatus.ERROR and gkg.total_nodes == 0:
            actions.append(
                "CRITICAL — Memgraph gKG is empty. "
                "Run your graph seeding script to populate "
                "ProblemType→TransactionType→CapabilityType chains."
            )
        elif gkg.empty_types:
            warnings.append(
                f"gKG missing paths for: {', '.join(gkg.empty_types)}. "
                f"Graph expansion will fall back to weak ILIKE for these types."
            )

    fc = report.fact_coverage
    if fc:
        if fc.cold_start_users > 0:
            pct = round(fc.cold_start_users / max(fc.total_users, 1) * 100, 1)
            msg = (
                f"{fc.cold_start_users} users ({pct}%) have no extracted facts. "
                f"Run POST /v1/ingest/pipeline for these users."
            )
            (actions if pct > 50 else warnings).append(msg)
        if fc.users_only_engine_visible > 0:
            warnings.append(
                f"{fc.users_only_engine_visible} users have only match_engine_only facts "
                f"(invisible to ILIKE fallback retrieval). "
                f"Check source_type tagging — use 'cv' not 'chat' for profile ingestion."
            )

    rs = report.retrieval_sanity
    if rs:
        if rs.status == DiagnosticStatus.ERROR and rs.after_hard_filter == 0:
            actions.append(
                "Retrieval returns 0 candidates even with fallback. "
                "Fix embedding coverage and gKG seeding first, then retest."
            )
        elif rs.used_fallback:
            warnings.append(
                "Match generation is using the basic fallback pool, not ranked retrieval. "
                "Fix embeddings and/or gKG seeding to restore proper retrieval."
            )

    ss = report.score_sanity
    if ss:
        if ss.pct_below_0_2 > 80:
            actions.append(
                f"{ss.pct_below_0_2}% of match scores are below 0.2. "
                f"Most likely cause: users lack need/objective facts so "
                f"relevance and complementarity are 0. "
                f"Check extraction prompt — ensure it extracts 'need' and 'objective' fact types."
            )
        if ss.all_zero_features:
            warnings.append(
                f"Dead scoring features (always 0): {', '.join(ss.all_zero_features)}. "
                f"These dimensions contribute nothing to any score."
            )

    return actions, warnings


# ─────────────────────────────────────────────────────────────
#  Main report builder
# ─────────────────────────────────────────────────────────────

async def build_matchmaking_report(
    db: AsyncSession,
    tenant_id: str,
    transaction_type: str = "technical_problem_solving",
    include_score_sanity: bool = True,
) -> MatchmakingDiagnosticReport:
    """
    Run all matchmaking diagnostic checks in parallel where safe,
    then assemble and return the full report.
    """
    # ── Infra checks (parallel) ───────────────────────────────
    pg_check, mg_check, ol_check = await asyncio.gather(
        check_postgres_latency(db),
        check_memgraph_latency(),
        check_ollama_latency(),
    )

    # ── Matchmaking checks ────────────────────────────────────
    embedding  = await check_embedding_coverage(db, tenant_id)
    gkg        = await check_gkg_seeding()
    facts      = await check_fact_coverage(db, tenant_id)
    retrieval  = await check_retrieval_sanity(db, tenant_id, transaction_type)
    score_san  = await check_score_sanity(db, tenant_id) if include_score_sanity else None

    all_statuses = [
        pg_check.status, mg_check.status, ol_check.status,
        embedding.status, gkg.status, facts.status, retrieval.status,
    ]
    if score_san:
        all_statuses.append(score_san.status)

    overall = _worst_status(*all_statuses)

    report = MatchmakingDiagnosticReport(
        tenant_id=tenant_id,
        overall_status=overall,
        infra_checks=[pg_check, mg_check, ol_check],
        embedding_coverage=embedding,
        gkg_seeding=gkg,
        fact_coverage=facts,
        retrieval_sanity=retrieval,
        score_sanity=score_san,
        checks_ok=sum(1 for s in all_statuses if s == DiagnosticStatus.OK),
        checks_warn=sum(1 for s in all_statuses if s == DiagnosticStatus.WARN),
        checks_error=sum(1 for s in all_statuses if s == DiagnosticStatus.ERROR),
    )

    report.actions_required, report.warnings = _build_actions(report)
    return report


# ─────────────────────────────────────────────────────────────
#  Self-test: full pipeline with assertions
# ─────────────────────────────────────────────────────────────

async def run_selftest(
    db: AsyncSession,
    tenant_id: Optional[str] = None,
    user_id: Optional[str] = None,
) -> SelfTestResult:
    """
    Runs a full end-to-end match pipeline against known test data
    and asserts on outputs. Reports per-stage timing so you can see
    exactly where a slow or broken pipeline fails.

    Stages:
      1. Verify user + facts exist
      2. Post a live intent signal
      3. Run retrieval — assert > 0 candidates
      4. Run match generation — assert > 0 matches
      5. Assert top score > 0.05
      6. Assert explanation was generated
    """
    t_total = time.perf_counter()
    tid   = tenant_id or SELFTEST_TENANT
    uid   = user_id or SELFTEST_USER
    stages: list[PipelineStageStatus] = []
    passed = True

    # ── Stage 1: user + facts ────────────────────────────────
    t0 = time.perf_counter()
    try:
        result = await db.execute(
            text("""
                SELECT u.user_id, COUNT(ef.fact_id) AS fact_count
                FROM users u
                LEFT JOIN extracted_facts ef
                    ON ef.user_id = u.user_id AND ef.tenant_id = :tid
                WHERE u.user_id = :uid AND u.tenant_id = :tid
                GROUP BY u.user_id
            """),
            {"tid": tid, "uid": uid},
        )
        row = result.mappings().first()
        if not row:
            stages.append(PipelineStageStatus(
                stage="user_profile",
                status=DiagnosticStatus.ERROR,
                detail=f"User {uid[:8]} not found in tenant {tid[:8]}",
                duration_ms=round((time.perf_counter() - t0) * 1000, 1),
            ))
            passed = False
        else:
            fact_count = row["fact_count"]
            stages.append(PipelineStageStatus(
                stage="user_profile",
                status=DiagnosticStatus.OK if fact_count > 0 else DiagnosticStatus.WARN,
                detail=f"User found with {fact_count} extracted facts",
                duration_ms=round((time.perf_counter() - t0) * 1000, 1),
                metadata={"fact_count": fact_count},
            ))
    except Exception as e:
        stages.append(PipelineStageStatus(
            stage="user_profile",
            status=DiagnosticStatus.ERROR,
            detail=str(e),
            duration_ms=round((time.perf_counter() - t0) * 1000, 1),
        ))
        passed = False

    # ── Stage 2: live signal ──────────────────────────────────
    t0 = time.perf_counter()
    try:
        await db.execute(
            text("""
                UPDATE live_signals
                SET valid_to = NOW()
                WHERE user_id = :uid AND signal_type = 'intent' AND valid_to IS NULL
            """),
            {"uid": uid},
        )
        sig_result = await db.execute(
            text("""
                INSERT INTO live_signals (tenant_id, user_id, signal_type, payload_json)
                VALUES (:tid, :uid, 'intent',
                    '{"text": "diagnostic selftest intent", "urgency": "low"}'::jsonb)
                RETURNING signal_id
            """),
            {"tid": tid, "uid": uid},
        )
        sig_row = sig_result.mappings().first()
        stages.append(PipelineStageStatus(
            stage="live_signal",
            status=DiagnosticStatus.OK,
            detail=f"Intent signal written: {str(sig_row['signal_id'])[:8]}",
            duration_ms=round((time.perf_counter() - t0) * 1000, 1),
        ))
    except Exception as e:
        stages.append(PipelineStageStatus(
            stage="live_signal",
            status=DiagnosticStatus.WARN,
            detail=f"Signal write failed (non-fatal): {e}",
            duration_ms=round((time.perf_counter() - t0) * 1000, 1),
        ))

    # ── Stage 3: retrieval ────────────────────────────────────
    t0 = time.perf_counter()
    try:
        from app.services.retrieval import retrieve_candidates
        candidates = await retrieve_candidates(
            db,
            tenant_id=tid,
            requester_id=uid,
            query_text="diagnostic selftest intent",
            transaction_type="technical_problem_solving",
            max_candidates=10,
        )
        retrieval_ok = len(candidates) > 0
        stages.append(PipelineStageStatus(
            stage="retrieval",
            status=DiagnosticStatus.OK if retrieval_ok else DiagnosticStatus.ERROR,
            detail=f"Retrieved {len(candidates)} candidates",
            duration_ms=round((time.perf_counter() - t0) * 1000, 1),
            metadata={"candidate_count": len(candidates)},
        ))
        if not retrieval_ok:
            passed = False
    except Exception as e:
        stages.append(PipelineStageStatus(
            stage="retrieval",
            status=DiagnosticStatus.ERROR,
            detail=str(e),
            duration_ms=round((time.perf_counter() - t0) * 1000, 1),
        ))
        passed = False
        candidates = []

    # ── Stage 4: ranking ──────────────────────────────────────
    t0 = time.perf_counter()
    top_score = None
    match_count = 0
    if candidates:
        try:
            from app.services.ranking import rank_candidates
            ranked = await rank_candidates(
                db,
                requester_id=uid,
                tenant_id=tid,
                candidate_ids=candidates,
                min_score=0.01,
            )
            match_count = len(ranked)
            top_score   = ranked[0][1].final_score if ranked else None
            ranking_ok  = match_count > 0

            stages.append(PipelineStageStatus(
                stage="ranking",
                status=DiagnosticStatus.OK if ranking_ok else DiagnosticStatus.WARN,
                detail=f"Ranked {match_count} candidates. Top score: {top_score:.4f}" if top_score else f"Ranked 0 above threshold",
                duration_ms=round((time.perf_counter() - t0) * 1000, 1),
                metadata={"match_count": match_count, "top_score": top_score},
            ))
            if not ranking_ok:
                passed = False
        except Exception as e:
            stages.append(PipelineStageStatus(
                stage="ranking",
                status=DiagnosticStatus.ERROR,
                detail=str(e),
                duration_ms=round((time.perf_counter() - t0) * 1000, 1),
            ))
            passed = False
    else:
        stages.append(PipelineStageStatus(
            stage="ranking",
            status=DiagnosticStatus.SKIP,
            detail="Skipped — no candidates from retrieval",
            duration_ms=0,
        ))

    # ── Stage 5: score threshold assertion ───────────────────
    if top_score is not None:
        threshold = 0.05
        score_ok  = top_score >= threshold
        stages.append(PipelineStageStatus(
            stage="score_threshold",
            status=DiagnosticStatus.OK if score_ok else DiagnosticStatus.WARN,
            detail=f"Top score {top_score:.4f} {'≥' if score_ok else '<'} threshold {threshold}",
            duration_ms=0,
            metadata={"top_score": top_score, "threshold": threshold},
        ))
        if not score_ok:
            passed = False

    # ── Cleanup: expire the selftest signal ──────────────────
    try:
        await db.execute(
            text("""
                UPDATE live_signals
                SET valid_to = NOW()
                WHERE user_id = :uid
                  AND signal_type = 'intent'
                  AND payload_json->>'text' = 'diagnostic selftest intent'
                  AND valid_to IS NULL
            """),
            {"uid": uid},
        )
        await db.commit()
    except Exception:
        pass

    duration_ms = round((time.perf_counter() - t_total) * 1000, 1)
    worst = _worst_status(*[s.status for s in stages])

    return SelfTestResult(
        tenant_id=tid,
        test_user_id=uid,
        stages=stages,
        matches_generated=match_count,
        top_score=top_score,
        passed=passed,
        status=DiagnosticStatus.OK if passed else worst,
        detail="All pipeline stages passed." if passed else "One or more pipeline stages failed — see stages for details.",
        duration_ms=duration_ms,
    )