"""
Delllo RAIN3.0 — Diagnostics Router

GET  /v1/diagnostics/health          Quick self-check (mirrors /health)
GET  /v1/diagnostics/stack           Infra dependency checks with latency
GET  /v1/diagnostics/report          Full matchmaking diagnostic report
GET  /v1/diagnostics/pipeline        Per-stage pipeline health summary
POST /v1/diagnostics/selftest        End-to-end match pipeline assertion test

All endpoints accept ?tenant_id= query param (defaults to first active tenant).
"""

import logging
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db.postgres import get_db
from .service import build_matchmaking_report, run_selftest
from .checks import (
    check_postgres_latency,
    check_memgraph_latency,
    check_ollama_latency,
    check_embedding_coverage,
    check_gkg_seeding,
    check_fact_coverage,
    check_retrieval_sanity,
    check_score_sanity,
)
from .models import DiagnosticStatus

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/diagnostics", tags=["diagnostics"])


# ─────────────────────────────────────────────────────────────
#  Helper: resolve tenant
# ─────────────────────────────────────────────────────────────

async def _resolve_tenant(db: AsyncSession, tenant_id: Optional[str]) -> str:
    if tenant_id:
        return tenant_id
    result = await db.execute(
        text("SELECT tenant_id FROM tenants WHERE status = 'active' ORDER BY created_at LIMIT 1")
    )
    row = result.mappings().first()
    if not row:
        return "00000000-0000-0000-0000-000000000001"
    return str(row["tenant_id"])


# ─────────────────────────────────────────────────────────────
#  GET /v1/diagnostics/health
# ─────────────────────────────────────────────────────────────

@router.get("/health", summary="Quick application self-check")
async def diagnostic_health():
    """Minimal liveness check. Always returns 200 if the app is running."""
    return {"status": "ok", "service": "delllo-diagnostics"}


# ─────────────────────────────────────────────────────────────
#  GET /v1/diagnostics/stack
# ─────────────────────────────────────────────────────────────

@router.get("/stack", summary="Infrastructure dependency checks with latency")
async def diagnostic_stack(db: AsyncSession = Depends(get_db)):
    """
    Runs Postgres, Memgraph, and Ollama checks with latency measurement.
    Unlike /health/stack, this also verifies that both required Ollama
    models (nomic-embed-text and the chat model) are available.
    """
    pg = await check_postgres_latency(db)
    mg = await check_memgraph_latency()
    ol = await check_ollama_latency()

    overall = "ok"
    if any(c.status == DiagnosticStatus.ERROR for c in [pg, mg, ol]):
        overall = "error"
    elif any(c.status == DiagnosticStatus.WARN for c in [pg, mg, ol]):
        overall = "warn"

    return {
        "overall": overall,
        "checks": {
            "postgres":  pg.model_dump(),
            "memgraph":  mg.model_dump(),
            "ollama":    ol.model_dump(),
        },
    }


# ─────────────────────────────────────────────────────────────
#  GET /v1/diagnostics/pipeline
# ─────────────────────────────────────────────────────────────

@router.get("/pipeline", summary="Matchmaking pipeline component health")
async def diagnostic_pipeline(
    tenant_id: Optional[str] = Query(default=None),
    transaction_type: str = Query(default="technical_problem_solving"),
    db: AsyncSession = Depends(get_db),
):
    """
    Runs the five matchmaking-specific checks and returns a concise
    per-stage summary. Faster than /report (no score sanity scan).

    Use this for monitoring dashboards and CI gates.
    """
    tid = await _resolve_tenant(db, tenant_id)

    embedding = await check_embedding_coverage(db, tid)
    gkg       = await check_gkg_seeding()
    facts     = await check_fact_coverage(db, tid)
    retrieval = await check_retrieval_sanity(db, tid, transaction_type)

    stages = [
        {"name": "embedding_coverage", "status": embedding.status,
         "detail": embedding.detail,
         "coverage_pct": embedding.coverage_pct},
        {"name": "gkg_seeding",        "status": gkg.status,
         "detail": gkg.detail,
         "seeded_types": gkg.seeded_types,
         "empty_types": gkg.empty_types},
        {"name": "fact_coverage",      "status": facts.status,
         "detail": facts.detail,
         "cold_start_users": facts.cold_start_users},
        {"name": "retrieval_sanity",   "status": retrieval.status,
         "detail": retrieval.detail,
         "semantic_hits": retrieval.semantic_hits,
         "graph_hits": retrieval.graph_hits,
         "final_candidates": retrieval.after_hard_filter,
         "used_fallback": retrieval.used_fallback},
    ]

    overall = "ok"
    if any(s["status"] == DiagnosticStatus.ERROR for s in stages):
        overall = "error"
    elif any(s["status"] == DiagnosticStatus.WARN for s in stages):
        overall = "warn"

    return {
        "tenant_id": tid,
        "transaction_type": transaction_type,
        "overall": overall,
        "stages": stages,
    }


# ─────────────────────────────────────────────────────────────
#  GET /v1/diagnostics/report
# ─────────────────────────────────────────────────────────────

@router.get("/report", summary="Full matchmaking diagnostic report")
async def diagnostic_report(
    tenant_id: Optional[str] = Query(default=None),
    transaction_type: str = Query(default="technical_problem_solving"),
    include_score_sanity: bool = Query(default=True),
    db: AsyncSession = Depends(get_db),
):
    """
    Full diagnostic report: infra checks + all five matchmaking checks
    + actionable remediation steps.

    Slower than /pipeline (includes score sanity DB scan) but gives
    the most complete picture. Run on-demand or after deployments.
    """
    tid    = await _resolve_tenant(db, tenant_id)
    report = await build_matchmaking_report(
        db,
        tenant_id=tid,
        transaction_type=transaction_type,
        include_score_sanity=include_score_sanity,
    )
    return report.model_dump()


# ─────────────────────────────────────────────────────────────
#  POST /v1/diagnostics/selftest
# ─────────────────────────────────────────────────────────────

@router.post("/selftest", summary="End-to-end pipeline assertion test")
async def diagnostic_selftest(
    tenant_id: Optional[str] = Query(default=None),
    user_id:   Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """
    Runs a full end-to-end pipeline test against a real or synthetic
    user and asserts on outputs at each stage:

      1. User + facts exist
      2. Live intent signal can be written
      3. Retrieval returns > 0 candidates
      4. Ranking produces > 0 scored matches
      5. Top score exceeds minimum threshold (0.05)

    Returns pass/fail per stage with timing so you can pinpoint exactly
    where the pipeline breaks. Cleans up the test signal after the run.

    Safe to call repeatedly — does not create permanent matches.
    """
    tid = await _resolve_tenant(db, tenant_id)
    result = await run_selftest(db, tenant_id=tid, user_id=user_id)
    return result.model_dump()