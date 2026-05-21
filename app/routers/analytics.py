"""
Delllo RAIN3.0 — Tenant Analytics Router (Phase 2)

GET /v1/analytics/{tenant_id}/overview      Match rates, acceptance rates, outcome stats
GET /v1/analytics/{tenant_id}/top-skills    Most common skills across tenant
GET /v1/analytics/{tenant_id}/match-quality Score distribution + feedback breakdown
GET /v1/analytics/{tenant_id}/coverage      Which users have facts, signals, matches
POST /v1/analytics/{tenant_id}/sweep        Trigger nightly learning sweep
"""

import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db.postgres import get_db
from app.services.feedback_learning import run_learning_sweep

logger = logging.getLogger(__name__)
router = APIRouter()


# ─────────────────────────────────────────────────────────────
#  GET /v1/analytics/{tenant_id}/overview
# ─────────────────────────────────────────────────────────────

@router.get("/analytics/{tenant_id}/overview")
async def get_overview(tenant_id: UUID, db: AsyncSession = Depends(get_db)):
    """
    High-level match pipeline stats for the tenant dashboard.
    """
    tid = str(tenant_id)

    stats = await db.execute(
        text("""
            SELECT
                COUNT(*)                                                    AS total_matches,
                COUNT(*) FILTER (WHERE status = 'recommended')              AS pending,
                COUNT(*) FILTER (WHERE status = 'accepted')                 AS accepted,
                COUNT(*) FILTER (WHERE status = 'dismissed')                AS dismissed,
                ROUND(AVG(score)::numeric, 3)                               AS avg_score,
                ROUND(MAX(score)::numeric, 3)                               AS max_score
            FROM matches
            WHERE tenant_id = :tid
        """),
        {"tid": tid},
    )
    match_row = stats.mappings().first()

    feedback = await db.execute(
        text("""
            SELECT
                feedback_type,
                COUNT(*) AS count
            FROM feedback_events fe
            JOIN matches m ON m.match_id = fe.match_id
            WHERE m.tenant_id = :tid
            GROUP BY feedback_type
            ORDER BY count DESC
        """),
        {"tid": tid},
    )
    feedback_rows = feedback.mappings().all()

    users = await db.execute(
        text("""
            SELECT
                COUNT(*)                                                AS total_users,
                COUNT(*) FILTER (WHERE status = 'active')              AS active_users
            FROM users WHERE tenant_id = :tid
        """),
        {"tid": tid},
    )
    user_row = users.mappings().first()

    facts = await db.execute(
        text("""
            SELECT
                COUNT(DISTINCT user_id)  AS users_with_facts,
                COUNT(*)                 AS total_facts,
                ROUND(AVG(confidence)::numeric, 3) AS avg_confidence
            FROM extracted_facts
            WHERE tenant_id = :tid
        """),
        {"tid": tid},
    )
    fact_row = facts.mappings().first()

    signals = await db.execute(
        text("""
            SELECT
                signal_type,
                COUNT(*) FILTER (WHERE valid_to IS NULL) AS active_count,
                COUNT(*)                                 AS total_count
            FROM live_signals
            WHERE tenant_id = :tid
            GROUP BY signal_type
        """),
        {"tid": tid},
    )
    signal_rows = signals.mappings().all()

    acceptance_rate = None
    total_m = match_row["total_matches"] or 0
    accepted_m = match_row["accepted"] or 0
    if total_m > 0:
        acceptance_rate = round(accepted_m / total_m, 3)

    return {
        "tenant_id": tid,
        "matches": {
            "total":           total_m,
            "pending":         match_row["pending"],
            "accepted":        accepted_m,
            "dismissed":       match_row["dismissed"],
            "avg_score":       match_row["avg_score"],
            "max_score":       match_row["max_score"],
            "acceptance_rate": acceptance_rate,
        },
        "users": {
            "total":  user_row["total_users"],
            "active": user_row["active_users"],
        },
        "facts": {
            "users_with_facts": fact_row["users_with_facts"],
            "total_facts":      fact_row["total_facts"],
            "avg_confidence":   fact_row["avg_confidence"],
        },
        "feedback": {r["feedback_type"]: r["count"] for r in feedback_rows},
        "signals":  [dict(r) for r in signal_rows],
    }


# ─────────────────────────────────────────────────────────────
#  GET /v1/analytics/{tenant_id}/top-skills
# ─────────────────────────────────────────────────────────────

@router.get("/analytics/{tenant_id}/top-skills")
async def get_top_skills(
    tenant_id: UUID,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
):
    """Top skills and domains by user count and average confidence."""
    tid = str(tenant_id)

    result = await db.execute(
        text("""
            SELECT
                fact_type,
                raw_value,
                COUNT(DISTINCT user_id)         AS user_count,
                ROUND(AVG(confidence)::numeric, 3) AS avg_confidence
            FROM extracted_facts
            WHERE tenant_id = :tid
              AND fact_type IN ('skill', 'domain', 'topic')
              AND visibility != 'private'
            GROUP BY fact_type, raw_value
            ORDER BY user_count DESC, avg_confidence DESC
            LIMIT :limit
        """),
        {"tid": tid, "limit": limit},
    )
    rows = result.mappings().all()

    by_type: dict = {}
    for r in rows:
        ft = r["fact_type"]
        by_type.setdefault(ft, []).append({
            "name":           r["raw_value"],
            "user_count":     r["user_count"],
            "avg_confidence": r["avg_confidence"],
        })

    return {"tenant_id": tid, "top_facts": by_type}


# ─────────────────────────────────────────────────────────────
#  GET /v1/analytics/{tenant_id}/match-quality
# ─────────────────────────────────────────────────────────────

@router.get("/analytics/{tenant_id}/match-quality")
async def get_match_quality(tenant_id: UUID, db: AsyncSession = Depends(get_db)):
    """
    Score distribution histogram and per-feature averages.
    Useful for tuning weights in ranking.py.
    """
    tid = str(tenant_id)

    score_dist = await db.execute(
        text("""
            SELECT
                CASE
                    WHEN score < 0.2 THEN '0.0–0.2'
                    WHEN score < 0.4 THEN '0.2–0.4'
                    WHEN score < 0.6 THEN '0.4–0.6'
                    WHEN score < 0.8 THEN '0.6–0.8'
                    ELSE                  '0.8–1.0'
                END AS bucket,
                COUNT(*) AS count
            FROM matches
            WHERE tenant_id = :tid
            GROUP BY bucket
            ORDER BY bucket
        """),
        {"tid": tid},
    )

    feature_avgs = await db.execute(
        text("""
            SELECT
                ROUND(AVG(ms.relevance)::numeric, 3)            AS avg_relevance,
                ROUND(AVG(ms.complementarity)::numeric, 3)      AS avg_complementarity,
                ROUND(AVG(ms.timing)::numeric, 3)               AS avg_timing,
                ROUND(AVG(ms.proximity)::numeric, 3)            AS avg_proximity,
                ROUND(AVG(ms.evidence_strength)::numeric, 3)    AS avg_evidence_strength,
                ROUND(AVG(ms.outcome_likelihood)::numeric, 3)   AS avg_outcome_likelihood,
                ROUND(AVG(ms.novelty)::numeric, 3)              AS avg_novelty,
                ROUND(AVG(ms.privacy_risk)::numeric, 3)         AS avg_privacy_risk,
                ROUND(AVG(ms.interaction_friction)::numeric, 3) AS avg_interaction_friction,
                score_version
            FROM match_scores ms
            JOIN matches m ON m.match_id = ms.match_id
            WHERE m.tenant_id = :tid
            GROUP BY score_version
            ORDER BY score_version
        """),
        {"tid": tid},
    )

    return {
        "tenant_id":       tid,
        "score_histogram": [dict(r) for r in score_dist.mappings().all()],
        "feature_averages": [dict(r) for r in feature_avgs.mappings().all()],
    }


# ─────────────────────────────────────────────────────────────
#  GET /v1/analytics/{tenant_id}/coverage
# ─────────────────────────────────────────────────────────────

@router.get("/analytics/{tenant_id}/coverage")
async def get_coverage(tenant_id: UUID, db: AsyncSession = Depends(get_db)):
    """
    Per-user coverage report — who has facts, signals, and matches.
    Useful for identifying cold-start users who need prompting.
    """
    tid = str(tenant_id)

    result = await db.execute(
        text("""
            SELECT
                u.user_id,
                u.display_name,
                u.status,
                COUNT(DISTINCT ef.fact_id)                             AS fact_count,
                COUNT(DISTINCT ls.signal_id)
                    FILTER (WHERE ls.valid_to IS NULL)                 AS active_signals,
                COUNT(DISTINCT m.match_id)
                    FILTER (WHERE m.status = 'recommended')            AS open_matches,
                COUNT(DISTINCT m.match_id)
                    FILTER (WHERE m.status = 'accepted')               AS accepted_matches,
                BOOL_OR(ef.fact_id IS NOT NULL)                        AS has_facts,
                BOOL_OR(ls.signal_id IS NOT NULL AND ls.valid_to IS NULL) AS has_active_signal
            FROM users u
            LEFT JOIN extracted_facts ef
                ON ef.user_id = u.user_id AND ef.tenant_id = :tid
            LEFT JOIN live_signals ls
                ON ls.user_id = u.user_id AND ls.tenant_id = :tid
            LEFT JOIN matches m
                ON m.person_a = u.user_id AND m.tenant_id = :tid
            WHERE u.tenant_id = :tid
            GROUP BY u.user_id, u.display_name, u.status
            ORDER BY fact_count DESC
        """),
        {"tid": tid},
    )
    rows = result.mappings().all()

    cold_start = [r for r in rows if not r["has_facts"]]
    no_signals = [r for r in rows if r["has_facts"] and not r["has_active_signal"]]

    return {
        "tenant_id":   tid,
        "users":       [dict(r) for r in rows],
        "cold_start_users":   len(cold_start),   # no facts at all
        "no_signal_users":    len(no_signals),   # has facts but no live signal
    }


# ─────────────────────────────────────────────────────────────
#  POST /v1/analytics/{tenant_id}/sweep
# ─────────────────────────────────────────────────────────────

@router.post("/analytics/{tenant_id}/sweep")
async def trigger_sweep(tenant_id: UUID, db: AsyncSession = Depends(get_db)):
    """
    Trigger the nightly learning sweep for this tenant.
    Recomputes outcome_likelihood snapshots for all active users.
    Can also be called from a Jenkins cron stage.
    """
    result = await run_learning_sweep(db, str(tenant_id))
    return result