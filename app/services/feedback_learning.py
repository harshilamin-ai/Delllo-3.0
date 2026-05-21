"""
Delllo RAIN3.0 — Feedback Learning Service (Phase 2)

Converts oKG interaction outcomes into ranking feature snapshots
stored in Postgres, so future matches benefit from past results.

What it does:
  1. After each feedback event (met, useful, no_show, not_useful),
     recompute the outcome_likelihood feature for that candidate.
  2. Store a feature_snapshot row in Postgres.
  3. The ranking engine reads these snapshots to pre-compute
     outcome_likelihood without hitting feedback_events every time.

Called from: matches.py submit_feedback()
Background:  run_learning_sweep() can be triggered nightly to
             recompute all user snapshots from full history.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  Outcome weights
# ─────────────────────────────────────────────────────────────

OUTCOME_WEIGHTS: dict[str, float] = {
    "met":       +1.0,
    "useful":    +0.8,
    "accepted":  +0.3,   # accepted but no meeting data yet
    "no_show":   -0.6,
    "not_useful":-0.4,
    "dismissed": -0.2,
}


# ─────────────────────────────────────────────────────────────
#  Per-user outcome score
# ─────────────────────────────────────────────────────────────

async def compute_user_outcome_score(
    db: AsyncSession,
    user_id: str,
    tenant_id: str,
) -> float:
    """
    Compute a 0–1 outcome score for a user based on their full
    feedback history across all matches.

    Uses exponential recency weighting so older events matter less.
    """
    result = await db.execute(
        text("""
            SELECT fe.feedback_type, fe.created_at
            FROM feedback_events fe
            JOIN matches m ON m.match_id = fe.match_id
            WHERE (m.person_a = :uid OR m.person_b = :uid)
              AND m.tenant_id = :tid
            ORDER BY fe.created_at DESC
            LIMIT 50
        """),
        {"uid": user_id, "tid": tenant_id},
    )
    rows = result.mappings().all()

    if not rows:
        return 0.5   # no history — neutral prior

    now        = datetime.now(timezone.utc)
    total_w    = 0.0
    weighted_s = 0.0
    decay      = 0.95   # each older event counts 5% less

    for i, row in enumerate(rows):
        ft     = row["feedback_type"]
        weight = decay ** i                      # recency weight
        score  = OUTCOME_WEIGHTS.get(ft, 0.0)   # outcome polarity

        weighted_s += score * weight
        total_w    += weight

    if total_w == 0:
        return 0.5

    # Normalise to [0, 1]: raw score is in [-1, 1]
    raw_normalised = (weighted_s / total_w + 1.0) / 2.0
    return max(0.0, min(1.0, raw_normalised))


# ─────────────────────────────────────────────────────────────
#  Feature snapshot write
# ─────────────────────────────────────────────────────────────

async def update_outcome_snapshot(
    db: AsyncSession,
    user_id: str,
    tenant_id: str,
) -> float:
    """
    Recompute outcome_likelihood for a user and upsert it into
    the feature_snapshots table. Returns the new score.

    The ranking engine reads from feature_snapshots first;
    falling back to live DB queries only when no snapshot exists.
    """
    score = await compute_user_outcome_score(db, user_id, tenant_id)

    try:
        await db.execute(
            text("""
                INSERT INTO feature_snapshots
                    (user_id, tenant_id, feature_name, feature_value, computed_at)
                VALUES
                    (:uid, :tid, 'outcome_likelihood', :score, NOW())
                ON CONFLICT (user_id, tenant_id, feature_name) DO UPDATE
                    SET feature_value = EXCLUDED.feature_value,
                        computed_at   = EXCLUDED.computed_at
            """),
            {"uid": user_id, "tid": tenant_id, "score": round(score, 4)},
        )
        logger.info(
            f"Outcome snapshot updated: user={user_id[:8]} "
            f"tenant={tenant_id[:8]} score={score:.3f}"
        )
    except Exception as e:
        logger.error(f"Failed to write feature snapshot for {user_id[:8]}: {e}")

    return score


# ─────────────────────────────────────────────────────────────
#  Called after each feedback event
# ─────────────────────────────────────────────────────────────

async def on_feedback_received(
    db: AsyncSession,
    *,
    match_id: str,
    feedback_type: str,
    tenant_id: str,
) -> None:
    """
    Triggered after a feedback event is written.
    Recomputes outcome_likelihood for both parties in the match.
    Non-fatal.
    """
    if feedback_type not in ("met", "useful", "not_useful", "no_show", "dismissed"):
        return   # only learning-relevant events

    try:
        result = await db.execute(
            text("SELECT person_a, person_b FROM matches WHERE match_id = :mid"),
            {"mid": match_id},
        )
        row = result.mappings().first()
        if not row:
            return

        for uid in [str(row["person_a"]), str(row["person_b"])]:
            await update_outcome_snapshot(db, uid, tenant_id)

    except Exception as e:
        logger.error(f"on_feedback_received failed for match {match_id[:8]}: {e}")


# ─────────────────────────────────────────────────────────────
#  Nightly sweep — recompute all user snapshots
# ─────────────────────────────────────────────────────────────

async def run_learning_sweep(db: AsyncSession, tenant_id: str) -> dict:
    """
    Recompute outcome_likelihood for every active user in a tenant.
    Run nightly via a scheduled job or Jenkins cron stage.
    """
    result = await db.execute(
        text("""
            SELECT DISTINCT u.user_id
            FROM users u
            WHERE u.tenant_id = :tid AND u.status = 'active'
        """),
        {"tid": tenant_id},
    )
    user_ids = [str(r["user_id"]) for r in result.mappings().all()]

    updated = 0
    errors  = []
    for uid in user_ids:
        try:
            await update_outcome_snapshot(db, uid, tenant_id)
            updated += 1
        except Exception as e:
            errors.append(f"user={uid[:8]}: {e}")

    logger.info(
        f"Learning sweep complete: tenant={tenant_id[:8]} "
        f"updated={updated} errors={len(errors)}"
    )
    return {
        "tenant_id":  tenant_id,
        "users_processed": len(user_ids),
        "snapshots_updated": updated,
        "errors": errors,
    }