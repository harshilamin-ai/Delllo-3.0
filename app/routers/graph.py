"""
Delllo RAIN3.0 — Graph API Router  (iKG + gKG endpoints)

iKG:
  POST /v1/ikg/upsert                      Re-sync user's iKG from PG facts
  GET  /v1/ikg/person/{person_id}          Full iKG subgraph for a person
  GET  /v1/ikg/person/{person_id}/evidence Evidence nodes for a person
  GET  /v1/ikg/person/{person_id}/signals  Active sKG signals for a person

gKG:
  GET  /v1/gkg/transaction-types           All TransactionType nodes
  GET  /v1/gkg/rules/{transaction_type_id} REQUIRES / BOOSTS / PENALISES edges
"""

import logging
import uuid as _uuid
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db.postgres import get_db
from app.db.graph import get_driver
from app.services import graph_writer

logger = logging.getLogger(__name__)
router = APIRouter()


class IKGUpsertRequest(BaseModel):
    user_id: UUID
    tenant_id: UUID


# ─────────────────────────────────────────────
#  POST /v1/ikg/upsert
# ─────────────────────────────────────────────

@router.post("/ikg/upsert", summary="Re-sync a user's iKG from extracted facts in Postgres")
async def upsert_ikg(
    req: IKGUpsertRequest,
    db: AsyncSession = Depends(get_db),
):
    user_id   = str(req.user_id)
    tenant_id = str(req.tenant_id)

    profile_result = await db.execute(
        text("""
            SELECT u.display_name, p.headline
            FROM users u
            LEFT JOIN user_profiles p ON p.user_id = u.user_id
            WHERE u.user_id = :uid
        """),
        {"uid": user_id},
    )
    profile = profile_result.mappings().first()
    if not profile:
        raise HTTPException(status_code=404, detail="User not found in tenant")

    facts_result = await db.execute(
        text("""
            SELECT fact_type, canonical_value, raw_value, confidence,
                   visibility, source_document_id
            FROM extracted_facts
            WHERE user_id = :uid AND tenant_id = :tid
            ORDER BY confidence DESC
        """),
        {"uid": user_id, "tid": tenant_id},
    )
    facts = facts_result.mappings().all()

    if not facts:
        return {"message": "No facts found. Run extraction first.", "nodes_written": 0}

    driver  = get_driver()
    written = 0
    errors  = []

    try:
        await graph_writer.upsert_person(
            driver, person_id=user_id, tenant_id=tenant_id,
            display_name=profile["display_name"],
            headline=profile["headline"] or "",
        )
        written += 1
    except Exception as e:
        errors.append(f"Person node: {type(e).__name__}: {e}")
        return {"user_id": user_id, "nodes_written": written,
                "facts_processed": 0, "errors": errors, "status": "error"}

    for fact in facts:
        ft   = fact["fact_type"]
        raw  = fact["raw_value"]
        can  = fact["canonical_value"]
        conf = float(fact["confidence"] or 0.7)
        vis  = fact["visibility"]

        try:
            if ft == "skill":
                await graph_writer.upsert_skill(
                    driver, person_id=user_id,
                    skill_name=raw, canonical_name=can,
                    confidence=conf, visibility=vis,
                )
            elif ft == "domain":
                await graph_writer.upsert_domain(
                    driver, person_id=user_id,
                    domain_name=raw, canonical_name=can,
                    confidence=conf, visibility=vis,
                )
            elif ft == "objective":
                obj_id = f"obj_{user_id}_{str(_uuid.uuid4())[:8]}"
                await graph_writer.upsert_objective(
                    driver, person_id=user_id, objective_id=obj_id,
                    text=raw, urgency="medium", visibility=vis,
                )
            elif ft == "offer":
                offer_id = f"offer_{user_id}_{str(_uuid.uuid4())[:8]}"
                await graph_writer.upsert_offer(
                    driver, person_id=user_id, offer_id=offer_id,
                    text=raw, confidence=conf, visibility=vis,
                )
            elif ft == "achievement":
                ach_id = f"ach_{user_id}_{str(_uuid.uuid4())[:8]}"
                await graph_writer.upsert_achievement(
                    driver, person_id=user_id, achievement_id=ach_id,
                    text=raw, confidence=conf, visibility=vis,
                )
            elif ft == "topic":
                await graph_writer.upsert_topic(
                    driver, person_id=user_id,
                    topic_name=raw, canonical_name=can,
                    confidence=conf, visibility=vis,
                )
            elif ft == "need":
                need_id = f"need_{user_id}_{str(_uuid.uuid4())[:8]}"
                await graph_writer.upsert_need(
                    driver, person_id=user_id, need_id=need_id,
                    text=raw, urgency="medium", visibility=vis,
                )
            elif ft == "asset":
                asset_id = f"asset_{user_id}_{str(_uuid.uuid4())[:8]}"
                await graph_writer.upsert_asset(
                    driver, person_id=user_id, asset_id=asset_id,
                    name=raw, asset_type="publication",
                    description=None, url_or_ref=None,
                    confidence=conf, visibility=vis,
                )
            elif ft == "project":
                proj_id = f"proj_{user_id}_{str(_uuid.uuid4())[:8]}"
                await graph_writer.upsert_project(
                    driver, person_id=user_id, project_id=proj_id,
                    name=raw, description=None, role=None,
                    organisation=None, confidence=conf, visibility=vis,
                )
            elif ft == "location":
                await graph_writer.upsert_location(
                    driver, person_id=user_id,
                    site=raw, floor=None, city=None,
                )
            elif ft == "constraint":
                con_id = f"con_{user_id}_{str(_uuid.uuid4())[:8]}"
                await graph_writer.upsert_constraint(
                    driver, person_id=user_id, constraint_id=con_id,
                    text=raw, constraint_type="availability", visibility=vis,
                )
            else:
                continue

            written += 1

        except Exception as e:
            err = f"{ft} '{can[:40]}': {type(e).__name__}: {e}"
            errors.append(err)
            logger.error(f"iKG upsert error: {err}")

    return {
        "user_id":         user_id,
        "nodes_written":   written,
        "facts_processed": len(facts),
        "errors":          errors,
        "status":          "ok" if not errors else "partial",
    }


# ─────────────────────────────────────────────
#  GET /v1/ikg/person/{person_id}
#
#  FIX: One query with 5 OPTIONAL MATCHes creates a Cartesian product:
#       6 skills × 1 domain × 7 objectives × 7 offers × 3 achievements = 882 rows.
#       collect(DISTINCT {node, edge_prop}) cannot deduplicate because the map
#       differs on edge properties from each cross-product row.
#       Solution: one query per relationship type.
# ─────────────────────────────────────────────

@router.get("/ikg/person/{person_id}", summary="Get a person's full iKG subgraph")
async def get_person_ikg(person_id: str):
    driver = get_driver()

    try:
        async with driver.session() as session:

            r = await session.run(
                "MATCH (p:Person {person_id: $pid}) RETURN p",
                pid=person_id,
            )
            rec = await r.single()
            if not rec or not rec["p"]:
                raise HTTPException(status_code=404,
                                    detail=f"Person {person_id} not found in graph")
            person_data = dict(rec["p"])

            r = await session.run(
                """
                MATCH (p:Person {person_id: $pid})-[rs:HAS_SKILL]->(s:Skill)
                RETURN s, rs.confidence AS confidence, rs.validated AS validated
                """,
                pid=person_id,
            )
            skills = [
                {**dict(rec["s"]),
                 "confidence": rec["confidence"],
                 "validated":  rec["validated"]}
                async for rec in r
            ]

            r = await session.run(
                """
                MATCH (p:Person {person_id: $pid})-[rd:HAS_DOMAIN]->(d:Domain)
                RETURN d, rd.confidence AS confidence
                """,
                pid=person_id,
            )
            domains = [
                {**dict(rec["d"]), "confidence": rec["confidence"]}
                async for rec in r
            ]

            r = await session.run(
                "MATCH (p:Person {person_id:$pid})-[:HAS_OBJECTIVE]->(o:Objective) RETURN o",
                pid=person_id,
            )
            objectives = [dict(rec["o"]) async for rec in r]

            r = await session.run(
                "MATCH (p:Person {person_id:$pid})-[:HAS_OFFER]->(of:Offer) RETURN of",
                pid=person_id,
            )
            offers = [dict(rec["of"]) async for rec in r]

            r = await session.run(
                "MATCH (p:Person {person_id:$pid})-[:ACHIEVED]->(a:Achievement) RETURN a",
                pid=person_id,
            )
            achievements = [dict(rec["a"]) async for rec in r]

        return {
            "person":       person_data,
            "skills":       skills,
            "domains":      domains,
            "objectives":   objectives,
            "offers":       offers,
            "achievements": achievements,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Graph read error for {person_id}: {e}")
        raise HTTPException(status_code=503,
                            detail=f"Graph query failed: {type(e).__name__}: {e}")


# ─────────────────────────────────────────────
#  GET /v1/ikg/person/{person_id}/evidence
# ─────────────────────────────────────────────

@router.get("/ikg/person/{person_id}/evidence",
            summary="Get evidence nodes for a person's claims")
async def get_person_evidence(person_id: str):
    driver = get_driver()
    try:
        async with driver.session() as session:
            r = await session.run(
                """
                MATCH (p:Person {person_id: $pid})-[:HAS_SKILL|HAS_DOMAIN]->(claim)
                MATCH (claim)-[:SUPPORTED_BY]->(e:Evidence)
                RETURN labels(claim)[0] AS claim_type,
                       claim.name       AS claim_name,
                       e
                ORDER BY e.confidence DESC
                """,
                pid=person_id,
            )
            records = await r.data()

        return {
            "person_id": person_id,
            "evidence": [
                {"claim_type": rec["claim_type"],
                 "claim_name": rec["claim_name"],
                 **dict(rec["e"])}
                for rec in records
            ],
        }
    except Exception as e:
        logger.error(f"Evidence query error for {person_id}: {e}")
        raise HTTPException(status_code=503,
                            detail=f"Graph query failed: {type(e).__name__}: {e}")


# ─────────────────────────────────────────────
#  GET /v1/ikg/person/{person_id}/signals
# ─────────────────────────────────────────────

@router.get("/ikg/person/{person_id}/signals",
            summary="Get active sKG signals for a person")
async def get_person_signals(person_id: str):
    driver = get_driver()
    try:
        async with driver.session() as session:
            r = await session.run(
                """
                MATCH (p:Person {person_id:$pid})-[:HAS_LIVE_INTENT]->(li:LiveIntent)
                WHERE li.valid_to IS NULL
                RETURN li
                """,
                pid=person_id,
            )
            intents = [dict(rec["li"]) async for rec in r]

            r = await session.run(
                """
                MATCH (p:Person {person_id:$pid})-[:PRESENT_AT]->(pr:Presence)
                WHERE pr.valid_to IS NULL
                RETURN pr
                """,
                pid=person_id,
            )
            presences = [dict(rec["pr"]) async for rec in r]

        return {"person_id": person_id, "intents": intents, "presences": presences}

    except Exception as e:
        logger.error(f"Signal query error for {person_id}: {e}")
        raise HTTPException(status_code=503,
                            detail=f"Graph query failed: {type(e).__name__}: {e}")


# ─────────────────────────────────────────────
#  GET /v1/gkg/transaction-types
# ─────────────────────────────────────────────

@router.get("/gkg/transaction-types", summary="List all gKG transaction types")
async def get_transaction_types():
    driver = get_driver()
    try:
        async with driver.session() as session:
            r = await session.run(
                "MATCH (tt:TransactionType) RETURN tt ORDER BY tt.name"
            )
            records = await r.data()
        return {"transaction_types": [dict(rec["tt"]) for rec in records]}
    except Exception as e:
        raise HTTPException(status_code=503,
                            detail=f"Graph query failed: {type(e).__name__}: {e}")


# ─────────────────────────────────────────────
#  GET /v1/gkg/rules/{transaction_type_id}
#
#  FIX: Original query mixed OPTIONAL MATCH (pt)-[req:REQUIRES]->(cap) with
#       OPTIONAL MATCH (ctx)-[boost:BOOSTS]->(tt) in one pass.
#       With 4 capabilities and 3 boosts this produces 4×3=12 boost rows.
#       Solution: separate query per edge type, use DISTINCT on each.
# ─────────────────────────────────────────────

@router.get("/gkg/rules/{transaction_type_id}",
            summary="Get scoring rules for a transaction type")
async def get_transaction_rules(transaction_type_id: str):
    driver = get_driver()

    try:
        async with driver.session() as session:

            r = await session.run(
                "MATCH (tt:TransactionType {type_id: $tid}) RETURN tt",
                tid=transaction_type_id,
            )
            rec = await r.single()
            if not rec or not rec["tt"]:
                raise HTTPException(
                    status_code=404,
                    detail=f"TransactionType '{transaction_type_id}' not found in gKG",
                )
            tt_data = dict(rec["tt"])

            r = await session.run(
                """
                MATCH (pt:ProblemType)-[:MAPS_TO]->
                      (:TransactionType {type_id: $tid})
                RETURN DISTINCT pt
                """,
                tid=transaction_type_id,
            )
            problem_types = [dict(rec["pt"]) async for rec in r]

            r = await session.run(
                """
                MATCH (pt:ProblemType)-[:MAPS_TO]->
                      (:TransactionType {type_id: $tid})
                MATCH (pt)-[req:REQUIRES]->(cap:CapabilityType)
                RETURN DISTINCT cap, req.weight AS weight
                """,
                tid=transaction_type_id,
            )
            requires = [
                {"capability": dict(rec["cap"]), "weight": rec["weight"]}
                async for rec in r
            ]

            r = await session.run(
                """
                MATCH (ctx:ContextType)-[boost:BOOSTS]->
                      (:TransactionType {type_id: $tid})
                RETURN DISTINCT ctx, boost.weight AS weight
                """,
                tid=transaction_type_id,
            )
            boosts = [
                {"context": dict(rec["ctx"]), "weight": rec["weight"]}
                async for rec in r
            ]

        return {
            "transaction_type": tt_data,
            "problem_types":    problem_types,
            "requires":         requires,
            "boosts":           boosts,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=503,
                            detail=f"Graph query failed: {type(e).__name__}: {e}")