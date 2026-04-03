"""
Delllo RAIN3.0 — Matches Router (Phase 2 — fully wired)

POST /v1/matches/generate              Retrieval → ranking → explanation
GET  /v1/matches/recommended           Ranked recommendations with full breakdown
GET  /v1/matches/{match_id}            Single match detail + explanation
POST /v1/matches/{match_id}/accept     Accept + oKG update
POST /v1/matches/{match_id}/dismiss    Dismiss + oKG update
POST /v1/matches/{match_id}/feedback   Feedback + oKG outcome + learning sweep
GET  /v1/matches/{match_id}/explanation LLM-generated explanation
"""

import json
import logging
import uuid
from typing import Optional, List, Dict, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db.postgres import get_db
from app.db.graph import get_driver
from app.services import graph_writer
from app.services.ranking import rank_candidates, load_profile
from app.services.retrieval import retrieve_candidates
from app.services.explanation import generate_and_store_explanation
from app.services.feedback_learning import on_feedback_received

logger = logging.getLogger(__name__)
router = APIRouter()


# ─────────────────────────────────────────────────────────────
#  Schemas
# ─────────────────────────────────────────────────────────────

class MatchGenerateRequest(BaseModel):
    tenant_id: UUID
    requesting_user_id: UUID
    transaction_types: List[str] = ["technical_problem_solving"]
    max_candidates: int = 20
    min_score: float = 0.05
    generate_explanations: bool = True    # set False to skip LLM for speed
    constraints: Dict[str, Any] = {}


class MatchFeedbackRequest(BaseModel):
    actor_user_id: UUID
    feedback_type: str
    payload: Dict[str, Any] = {}


# ─────────────────────────────────────────────────────────────
#  POST /v1/matches/generate
# ─────────────────────────────────────────────────────────────

@router.post("/matches/generate", summary="Generate match recommendations (Phase 2)")
async def generate_matches(
    req: MatchGenerateRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Full Phase 2 pipeline:
      1. Retrieval    — pgvector semantic search + gKG graph expansion
      2. Hard filter  — active, no open match, non-private facts
      3. Ranking      — 9-feature deterministic score
      4. Persist      — match + full score breakdown
      5. Explanation  — LLM narrative + agenda (async, non-fatal)
      6. oKG write    — MatchRecommendation node in Memgraph
    """
    requester_id = str(req.requesting_user_id)
    tenant_id    = str(req.tenant_id)
    tx_type      = req.transaction_types[0] if req.transaction_types else "knowledge_transfer"

    # ── Load requester profile for query text ─────────────────
    requester_profile = await load_profile(db, requester_id, tenant_id)

    # Build query text from live intent or needs/objectives
    query_text = requester_profile.intent_text
    if not query_text:
        query_text = " ".join(list(requester_profile.needs)[:3]
                              + list(requester_profile.objectives)[:2]
                              + list(requester_profile.skills)[:3])
    if not query_text:
        query_text = tx_type.replace("_", " ")

    # ── Step 1+2: Retrieve + filter candidates ────────────────
    candidate_ids = await retrieve_candidates(
        db,
        tenant_id=tenant_id,
        requester_id=requester_id,
        query_text=query_text,
        transaction_type=tx_type,
        max_candidates=req.max_candidates * 3,
    )

    # Fallback: if retrieval found nothing (e.g. no embeddings yet),
    # fall back to basic active-user query
    if not candidate_ids:
        logger.warning("Retrieval returned no candidates — using basic fallback pool")
        pool_result = await db.execute(
            text("""
                SELECT DISTINCT u.user_id
                FROM users u
                WHERE u.tenant_id = :tid
                  AND u.user_id  != :rid
                  AND u.status    = 'active'
                  AND EXISTS (
                      SELECT 1 FROM extracted_facts ef
                      WHERE ef.user_id   = u.user_id
                        AND ef.tenant_id = :tid
                        AND ef.visibility != 'private'
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM matches m
                      WHERE m.tenant_id = :tid
                        AND m.person_a  = :rid
                        AND m.person_b  = u.user_id
                        AND m.status NOT IN ('expired', 'dismissed')
                  )
                LIMIT :limit
            """),
            {"tid": tenant_id, "rid": requester_id,
             "limit": req.max_candidates * 3},
        )
        candidate_ids = [str(r["user_id"]) for r in pool_result.mappings().all()]

    if not candidate_ids:
        return {
            "message": "No candidates found.",
            "matches_created": 0,
            "matches": [],
            "score_version": "v2.0",
        }

    # ── Step 3: Rank ──────────────────────────────────────────
    logger.info(
        f"Ranking {len(candidate_ids)} candidates for "
        f"requester={requester_id[:8]} tenant={tenant_id[:8]}"
    )
    ranked = await rank_candidates(
        db,
        requester_id=requester_id,
        tenant_id=tenant_id,
        candidate_ids=candidate_ids,
        min_score=req.min_score,
    )
    ranked = ranked[: req.max_candidates]

    if not ranked:
        return {
            "message": "All candidates scored below minimum threshold.",
            "matches_created": 0,
            "matches": [],
            "score_version": "v2.0",
        }

    # ── Steps 4+5+6: Persist, explain, oKG ───────────────────
    driver         = get_driver()
    created_matches = []

    for candidate_id, breakdown in ranked:
        match_id = str(uuid.uuid4())
        score    = breakdown.final_score
        bd       = breakdown.to_dict()

        # Fetch display meta
        meta_result = await db.execute(
            text("""
                SELECT u.display_name, p.headline
                FROM users u
                LEFT JOIN user_profiles p ON p.user_id = u.user_id
                WHERE u.user_id = :uid
            """),
            {"uid": candidate_id},
        )
        meta = meta_result.mappings().first() or {}

        # Postgres: match
        await db.execute(
            text("""
                INSERT INTO matches
                    (match_id, tenant_id, person_a, person_b,
                     transaction_type, score, status)
                VALUES
                    (:match_id, :tid, :pa, :pb, :tx_type, :score, 'recommended')
            """),
            {
                "match_id": match_id, "tid": tenant_id,
                "pa": requester_id,   "pb": candidate_id,
                "tx_type": tx_type,   "score": round(score, 4),
            },
        )

        # Postgres: score breakdown
        await db.execute(
            text("""
                INSERT INTO match_scores (
                    match_id, relevance, complementarity, timing, proximity,
                    evidence_strength, outcome_likelihood, novelty,
                    privacy_risk, interaction_friction, score_version
                ) VALUES (
                    :match_id, :relevance, :complementarity, :timing, :proximity,
                    :evidence_strength, :outcome_likelihood, :novelty,
                    :privacy_risk, :interaction_friction, 'v2.0'
                )
            """),
            {"match_id": match_id, **{k: v for k, v in bd.items()
                                      if k != "final_score"}},
        )

        # Explanation (non-fatal, non-blocking)
        explanation = {}
        if req.generate_explanations:
            try:
                candidate_profile = await load_profile(db, candidate_id, tenant_id)
                explanation = await generate_and_store_explanation(
                    db,
                    match_id=match_id,
                    requester=requester_profile,
                    candidate=candidate_profile,
                    score=score,
                    score_breakdown=bd,
                    transaction_type=tx_type,
                )
            except Exception as e:
                logger.warning(f"Explanation skipped for {match_id[:8]}: {e}")

        # oKG
        try:
            await graph_writer.upsert_match_recommendation(
                driver,
                match_id=match_id,
                person_a_id=requester_id,
                person_b_id=candidate_id,
                score=score,
                transaction_type=tx_type,
                status="recommended",
            )
        except Exception as e:
            logger.warning(f"oKG match write failed (non-fatal): {e}")

        created_matches.append({
            "match_id":           match_id,
            "candidate_id":       candidate_id,
            "candidate_name":     meta.get("display_name", ""),
            "candidate_headline": meta.get("headline", ""),
            "score":              round(score, 4),
            "score_breakdown":    bd,
            "explanation":        explanation,
            "transaction_type":   tx_type,
            "status":             "recommended",
            "score_version":      "v2.0",
        })

    return {
        "matches_created":      len(created_matches),
        "matches":              created_matches,
        "score_version":        "v2.0",
        "candidates_evaluated": len(candidate_ids),
        "query_text":           query_text[:100],
    }


# ─────────────────────────────────────────────────────────────
#  GET /v1/matches/recommended
# ─────────────────────────────────────────────────────────────

@router.get("/matches/recommended", summary="Get recommended matches")
async def get_recommended(
    user_id:   UUID = Query(...),
    tenant_id: UUID = Query(...),
    limit: int = Query(default=10, le=50),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        text("""
            SELECT
                m.match_id, m.person_b AS candidate_id, m.transaction_type,
                m.score, m.status, m.created_at,
                u.display_name AS candidate_name,
                p.headline     AS candidate_headline,
                ms.relevance, ms.complementarity, ms.timing,
                ms.proximity, ms.evidence_strength, ms.outcome_likelihood,
                ms.novelty, ms.privacy_risk, ms.interaction_friction,
                ms.score_version,
                e.explanation_text, e.agenda_text, e.opening_question
            FROM matches m
            JOIN users u ON u.user_id = m.person_b
            LEFT JOIN user_profiles p  ON p.user_id  = m.person_b
            LEFT JOIN match_scores  ms ON ms.match_id = m.match_id
            LEFT JOIN explanations  e  ON e.match_id  = m.match_id
            WHERE m.person_a  = :uid
              AND m.tenant_id = :tid
              AND m.status    = 'recommended'
            ORDER BY m.score DESC
            LIMIT :limit
        """),
        {"uid": str(user_id), "tid": str(tenant_id), "limit": limit},
    )
    rows = result.mappings().all()
    return {"user_id": str(user_id), "recommended": [dict(r) for r in rows], "count": len(rows)}


# ─────────────────────────────────────────────────────────────
#  GET /v1/matches/{match_id}
# ─────────────────────────────────────────────────────────────

@router.get("/matches/{match_id}", summary="Get match detail with explanation")
async def get_match(match_id: UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("""
            SELECT
                m.match_id, m.tenant_id, m.person_a, m.person_b,
                m.transaction_type, m.score, m.status, m.created_at,
                ua.display_name AS person_a_name,
                ub.display_name AS person_b_name,
                ms.relevance, ms.complementarity, ms.timing, ms.proximity,
                ms.evidence_strength, ms.outcome_likelihood,
                ms.novelty, ms.privacy_risk, ms.interaction_friction,
                ms.score_version,
                e.explanation_text, e.agenda_text, e.opening_question, e.model_used
            FROM matches m
            JOIN users ua ON ua.user_id = m.person_a
            JOIN users ub ON ub.user_id = m.person_b
            LEFT JOIN match_scores ms ON ms.match_id = m.match_id
            LEFT JOIN explanations e  ON e.match_id  = m.match_id
            WHERE m.match_id = :mid
        """),
        {"mid": str(match_id)},
    )
    row = result.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Match not found")
    return dict(row)


# ─────────────────────────────────────────────────────────────
#  POST /v1/matches/{match_id}/accept
# ─────────────────────────────────────────────────────────────

@router.post("/matches/{match_id}/accept")
async def accept_match(
    match_id: UUID,
    actor_user_id: UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    mid = str(match_id)
    result = await db.execute(
        text("SELECT status FROM matches WHERE match_id = :mid"), {"mid": mid}
    )
    row = result.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Match not found")
    if row["status"] != "recommended":
        raise HTTPException(status_code=409, detail=f"Match is already '{row['status']}'")

    await db.execute(
        text("UPDATE matches SET status = 'accepted' WHERE match_id = :mid"), {"mid": mid}
    )
    await db.execute(
        text("""
            INSERT INTO feedback_events (tenant_id, match_id, actor_user_id, feedback_type)
            SELECT tenant_id, match_id, :actor_id, 'accepted'
            FROM matches WHERE match_id = :mid
        """),
        {"mid": mid, "actor_id": str(actor_user_id)},
    )
    try:
        await graph_writer.update_match_status(get_driver(), match_id=mid, status="accepted")
    except Exception as e:
        logger.warning(f"oKG accept update failed (non-fatal): {e}")

    return {"match_id": mid, "status": "accepted"}


# ─────────────────────────────────────────────────────────────
#  POST /v1/matches/{match_id}/dismiss
# ─────────────────────────────────────────────────────────────

@router.post("/matches/{match_id}/dismiss")
async def dismiss_match(
    match_id: UUID,
    actor_user_id: UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    mid = str(match_id)
    await db.execute(
        text("UPDATE matches SET status = 'dismissed' WHERE match_id = :mid"), {"mid": mid}
    )
    await db.execute(
        text("""
            INSERT INTO feedback_events (tenant_id, match_id, actor_user_id, feedback_type)
            SELECT tenant_id, match_id, :actor_id, 'dismissed'
            FROM matches WHERE match_id = :mid
        """),
        {"mid": mid, "actor_id": str(actor_user_id)},
    )
    try:
        await graph_writer.update_match_status(get_driver(), match_id=mid, status="dismissed")
    except Exception as e:
        logger.warning(f"oKG dismiss update failed (non-fatal): {e}")

    return {"match_id": mid, "status": "dismissed"}


# ─────────────────────────────────────────────────────────────
#  POST /v1/matches/{match_id}/feedback
# ─────────────────────────────────────────────────────────────

@router.post("/matches/{match_id}/feedback")
async def submit_feedback(
    match_id: UUID,
    req: MatchFeedbackRequest,
    db: AsyncSession = Depends(get_db),
):
    mid = str(match_id)
    valid_types = {"accepted", "dismissed", "useful", "not_useful", "met", "no_show"}
    if req.feedback_type not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"feedback_type must be one of: {', '.join(sorted(valid_types))}",
        )

    result = await db.execute(
        text("SELECT tenant_id FROM matches WHERE match_id = :mid"), {"mid": mid}
    )
    row = result.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Match not found")

    tenant_id = str(row["tenant_id"])

    await db.execute(
        text("""
            INSERT INTO feedback_events
                (tenant_id, match_id, actor_user_id, feedback_type, payload_json)
            VALUES (:tid, :mid, :actor_id, :ftype, CAST(:payload AS JSONB))
        """),
        {
            "tid":      tenant_id,
            "mid":      mid,
            "actor_id": str(req.actor_user_id),
            "ftype":    req.feedback_type,
            "payload":  json.dumps(req.payload),
        },
    )

    # oKG outcome
    if req.feedback_type in ("met", "no_show", "useful", "not_useful"):
        quality_score = req.payload.get("quality_score")
        outcome_id = f"outcome_{str(uuid.uuid4())[:8]}"
        try:
            await graph_writer.upsert_interaction_outcome(
                get_driver(),
                match_id=mid,
                outcome_id=outcome_id,
                outcome_type=req.feedback_type,
                quality_score=float(quality_score) if quality_score is not None else None,
            )
        except Exception as e:
            logger.warning(f"oKG outcome write failed (non-fatal): {e}")

    # Feedback learning — update outcome_likelihood snapshots
    await on_feedback_received(
        db,
        match_id=mid,
        feedback_type=req.feedback_type,
        tenant_id=tenant_id,
    )

    return {"match_id": mid, "feedback_type": req.feedback_type, "status": "recorded"}


# ─────────────────────────────────────────────────────────────
#  GET /v1/matches/{match_id}/explanation
# ─────────────────────────────────────────────────────────────

@router.get("/matches/{match_id}/explanation")
async def get_explanation(match_id: UUID, db: AsyncSession = Depends(get_db)):
    mid = str(match_id)
    result = await db.execute(
        text("""
            SELECT e.explanation_text, e.agenda_text,
                   e.opening_question, e.model_used
            FROM explanations e WHERE e.match_id = :mid
        """),
        {"mid": mid},
    )
    row = result.mappings().first()
    if row and row["explanation_text"]:
        return dict(row)

    # Explanation not yet generated — trigger it now
    match_result = await db.execute(
        text("""
            SELECT m.tenant_id, m.person_a, m.person_b,
                   m.transaction_type, m.score,
                   ms.relevance, ms.complementarity, ms.timing,
                   ms.evidence_strength, ms.outcome_likelihood
            FROM matches m
            LEFT JOIN match_scores ms ON ms.match_id = m.match_id
            WHERE m.match_id = :mid
        """),
        {"mid": mid},
    )
    match_row = match_result.mappings().first()
    if not match_row:
        raise HTTPException(status_code=404, detail="Match not found")

    try:
        requester  = await load_profile(db, str(match_row["person_a"]),
                                        str(match_row["tenant_id"]))
        candidate  = await load_profile(db, str(match_row["person_b"]),
                                        str(match_row["tenant_id"]))
        explanation = await generate_and_store_explanation(
            db,
            match_id=mid,
            requester=requester,
            candidate=candidate,
            score=float(match_row["score"] or 0.5),
            score_breakdown={
                "relevance":          float(match_row["relevance"] or 0),
                "complementarity":    float(match_row["complementarity"] or 0),
                "timing":             float(match_row["timing"] or 0),
                "evidence_strength":  float(match_row["evidence_strength"] or 0),
                "outcome_likelihood": float(match_row["outcome_likelihood"] or 0),
            },
            transaction_type=str(match_row["transaction_type"]),
        )
        return {**explanation, "match_id": mid}
    except Exception as e:
        logger.error(f"On-demand explanation failed: {e}")
        return {
            "match_id":         mid,
            "explanation_text": None,
            "agenda_text":      None,
            "opening_question": None,
            "error":            str(e),
        }