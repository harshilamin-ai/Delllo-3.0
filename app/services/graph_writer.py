"""
Delllo RAIN3.0 — Graph Writer Service
Mirrors extracted facts, signals, and match outcomes into Memgraph.

Covers all four KG layers:
  iKG  — Person, Skill, Domain, Objective, Offer, Achievement, Evidence
  gKG  — (seeded by init_graph.py; writer handles capability linking)
  sKG  — LiveIntent, Presence, Session
  oKG  — MatchRecommendation, InteractionOutcome

Called from:
  extraction.py  → write_extraction_to_ikg()
  signals.py     → write_signal_to_skg()
  matches.py     → write_match_to_okg() / write_outcome_to_okg()
"""

from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Optional

from neo4j import AsyncDriver

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()

def _make_id() -> str:
    return str(uuid.uuid4())


# ─────────────────────────────────────────────
#  iKG Primitives
# ─────────────────────────────────────────────

async def upsert_person(
    driver: AsyncDriver,
    *,
    person_id: str,
    tenant_id: str,
    display_name: str,
    headline: str = "",
    visibility: str = "match_engine_only",
) -> None:
    async with driver.session() as session:
        result = await session.run(
            """
            MERGE (p:Person {person_id: $person_id})
            SET p.tenant_id    = $tenant_id,
                p.display_name = $display_name,
                p.headline     = $headline,
                p.visibility   = $visibility,
                p.updated_at   = $now
            """,
            person_id=person_id, tenant_id=tenant_id, display_name=display_name,
            headline=headline, visibility=visibility, now=_now(),
        )
        await result.consume()


async def upsert_skill(
    driver: AsyncDriver,
    *,
    person_id: str,
    skill_name: str,
    canonical_name: str,
    confidence: float,
    visibility: str,
    validated: bool = False,
    evidence_id: Optional[str] = None,
) -> None:
    skill_id = f"skill_{canonical_name}"
    async with driver.session() as session:
        # Upsert Skill node
        r = await session.run(
            """
            MERGE (s:Skill {skill_id: $skill_id})
            SET s.name           = $skill_name,
                s.canonical_name = $canonical_name
            """,
            skill_id=skill_id, skill_name=skill_name, canonical_name=canonical_name,
        )
        await r.consume()

        # Upsert HAS_SKILL edge
        r = await session.run(
            """
            MATCH (p:Person {person_id: $person_id})
            MATCH (s:Skill  {skill_id:  $skill_id})
            MERGE (p)-[r:HAS_SKILL]->(s)
            SET r.confidence = $confidence,
                r.visibility = $visibility,
                r.validated  = $validated,
                r.updated_at = $now
            """,
            person_id=person_id, skill_id=skill_id, confidence=confidence,
            visibility=visibility, validated=validated, now=_now(),
        )
        await r.consume()

        # Link to Evidence if provided
        if evidence_id:
            r = await session.run(
                """
                MATCH (s:Skill    {skill_id:    $skill_id})
                MATCH (e:Evidence {evidence_id: $evidence_id})
                MERGE (s)-[:SUPPORTED_BY]->(e)
                """,
                skill_id=skill_id, evidence_id=evidence_id,
            )
            await r.consume()

        # Try to link Skill → CapabilityType in gKG by canonical name
        r = await session.run(
            """
            MATCH (s:Skill         {skill_id:      $skill_id})
            MATCH (c:CapabilityType {canonical_name: $canonical_name})
            MERGE (s)-[:MAPS_TO_CAPABILITY]->(c)
            """,
            skill_id=skill_id, canonical_name=canonical_name,
        )
        await r.consume()


async def upsert_domain(
    driver: AsyncDriver,
    *,
    person_id: str,
    domain_name: str,
    canonical_name: str,
    confidence: float,
    visibility: str,
    evidence_id: Optional[str] = None,
) -> None:
    domain_id = f"domain_{canonical_name}"
    async with driver.session() as session:
        r = await session.run(
            """
            MERGE (d:Domain {domain_id: $domain_id})
            SET d.name           = $domain_name,
                d.canonical_name = $canonical_name
            """,
            domain_id=domain_id, domain_name=domain_name, canonical_name=canonical_name,
        )
        await r.consume()

        r = await session.run(
            """
            MATCH (p:Person {person_id: $person_id})
            MATCH (d:Domain {domain_id: $domain_id})
            MERGE (p)-[r:HAS_DOMAIN]->(d)
            SET r.confidence = $confidence,
                r.visibility = $visibility,
                r.updated_at = $now
            """,
            person_id=person_id, domain_id=domain_id, confidence=confidence,
            visibility=visibility, now=_now(),
        )
        await r.consume()

        if evidence_id:
            r = await session.run(
                """
                MATCH (d:Domain   {domain_id:   $domain_id})
                MATCH (e:Evidence {evidence_id: $evidence_id})
                MERGE (d)-[:SUPPORTED_BY]->(e)
                """,
                domain_id=domain_id, evidence_id=evidence_id,
            )
            await r.consume()


async def upsert_objective(
    driver: AsyncDriver,
    *,
    person_id: str,
    objective_id: str,
    text: str,
    urgency: str = "medium",
    visibility: str = "match_engine_only",
    valid_until: Optional[str] = None,
) -> None:
    async with driver.session() as session:
        r = await session.run(
            """
            MERGE (o:Objective {objective_id: $objective_id})
            SET o.text        = $text,
                o.status      = 'active',
                o.urgency     = $urgency,
                o.valid_until = $valid_until,
                o.visibility  = $visibility,
                o.updated_at  = $now
            """,
            objective_id=objective_id, text=text, urgency=urgency,
            valid_until=valid_until, visibility=visibility, now=_now(),
        )
        await r.consume()

        r = await session.run(
            """
            MATCH (p:Person    {person_id:    $person_id})
            MATCH (o:Objective {objective_id: $objective_id})
            MERGE (p)-[:HAS_OBJECTIVE]->(o)
            """,
            person_id=person_id, objective_id=objective_id,
        )
        await r.consume()


async def upsert_offer(
    driver: AsyncDriver,
    *,
    person_id: str,
    offer_id: str,
    text: str,
    confidence: float,
    visibility: str,
) -> None:
    async with driver.session() as session:
        r = await session.run(
            """
            MERGE (of:Offer {offer_id: $offer_id})
            SET of.text       = $text,
                of.confidence = $confidence,
                of.visibility = $visibility,
                of.updated_at = $now
            """,
            offer_id=offer_id, text=text, confidence=confidence,
            visibility=visibility, now=_now(),
        )
        await r.consume()

        r = await session.run(
            """
            MATCH (p:Person {person_id: $person_id})
            MATCH (of:Offer {offer_id:  $offer_id})
            MERGE (p)-[:HAS_OFFER]->(of)
            """,
            person_id=person_id, offer_id=offer_id,
        )
        await r.consume()


async def upsert_achievement(
    driver: AsyncDriver,
    *,
    person_id: str,
    achievement_id: str,
    text: str,
    confidence: float,
    visibility: str,
) -> None:
    async with driver.session() as session:
        r = await session.run(
            """
            MERGE (a:Achievement {achievement_id: $achievement_id})
            SET a.text        = $text,
                a.confidence  = $confidence,
                a.visibility  = $visibility,
                a.updated_at  = $now
            """,
            achievement_id=achievement_id, text=text, confidence=confidence,
            visibility=visibility, now=_now(),
        )
        await r.consume()

        r = await session.run(
            """
            MATCH (p:Person      {person_id:      $person_id})
            MATCH (a:Achievement {achievement_id: $achievement_id})
            MERGE (p)-[:ACHIEVED]->(a)
            """,
            person_id=person_id, achievement_id=achievement_id,
        )
        await r.consume()


async def upsert_evidence(
    driver: AsyncDriver,
    *,
    evidence_id: str,
    claim_type: str,
    source_document_id: str,
    confidence: float,
    freshness_date: str,
    visibility: str,
    source_chunk_id: Optional[str] = None,
) -> None:
    async with driver.session() as session:
        r = await session.run(
            """
            MERGE (e:Evidence {evidence_id: $evidence_id})
            SET e.claim_type         = $claim_type,
                e.source_document_id = $source_document_id,
                e.source_chunk_id    = $source_chunk_id,
                e.confidence         = $confidence,
                e.freshness_date     = $freshness_date,
                e.visibility         = $visibility
            """,
            evidence_id=evidence_id, claim_type=claim_type,
            source_document_id=source_document_id, source_chunk_id=source_chunk_id,
            confidence=confidence, freshness_date=freshness_date, visibility=visibility,
        )
        await r.consume()


async def upsert_topic(
    driver: AsyncDriver,
    *,
    person_id: str,
    topic_name: str,
    canonical_name: str,
    confidence: float,
    visibility: str,
) -> None:
    topic_id = f"topic_{canonical_name}"
    async with driver.session() as session:
        r = await session.run(
            """
            MERGE (t:Topic {topic_id: $topic_id})
            SET t.name           = $topic_name,
                t.canonical_name = $canonical_name
            """,
            topic_id=topic_id, topic_name=topic_name, canonical_name=canonical_name,
        )
        await r.consume()
        r = await session.run(
            """
            MATCH (p:Person {person_id: $person_id})
            MATCH (t:Topic  {topic_id:  $topic_id})
            MERGE (p)-[r:HAS_TOPIC]->(t)
            SET r.confidence = $confidence,
                r.visibility = $visibility,
                r.updated_at = $now
            """,
            person_id=person_id, topic_id=topic_id, confidence=confidence,
            visibility=visibility, now=_now(),
        )
        await r.consume()


async def upsert_need(
    driver: AsyncDriver,
    *,
    person_id: str,
    need_id: str,
    text: str,
    urgency: str = "medium",
    visibility: str = "match_engine_only",
) -> None:
    async with driver.session() as session:
        r = await session.run(
            """
            MERGE (n:Need {need_id: $need_id})
            SET n.text       = $text,
                n.urgency    = $urgency,
                n.status     = 'active',
                n.visibility = $visibility,
                n.updated_at = $now
            """,
            need_id=need_id, text=text, urgency=urgency,
            visibility=visibility, now=_now(),
        )
        await r.consume()
        r = await session.run(
            """
            MATCH (p:Person {person_id: $person_id})
            MATCH (n:Need   {need_id:   $need_id})
            MERGE (p)-[:HAS_NEED]->(n)
            """,
            person_id=person_id, need_id=need_id,
        )
        await r.consume()


async def upsert_asset(
    driver: AsyncDriver,
    *,
    person_id: str,
    asset_id: str,
    name: str,
    asset_type: str,
    description: Optional[str],
    url_or_ref: Optional[str],
    confidence: float,
    visibility: str,
) -> None:
    async with driver.session() as session:
        r = await session.run(
            """
            MERGE (a:Asset {asset_id: $asset_id})
            SET a.name        = $name,
                a.asset_type  = $asset_type,
                a.description = $description,
                a.url_or_ref  = $url_or_ref,
                a.confidence  = $confidence,
                a.visibility  = $visibility,
                a.updated_at  = $now
            """,
            asset_id=asset_id, name=name, asset_type=asset_type, description=description,
            url_or_ref=url_or_ref, confidence=confidence, visibility=visibility, now=_now(),
        )
        await r.consume()
        r = await session.run(
            """
            MATCH (p:Person {person_id: $person_id})
            MATCH (a:Asset  {asset_id:  $asset_id})
            MERGE (p)-[:AUTHORED]->(a)
            """,
            person_id=person_id, asset_id=asset_id,
        )
        await r.consume()


async def upsert_project(
    driver: AsyncDriver,
    *,
    person_id: str,
    project_id: str,
    name: str,
    description: Optional[str],
    role: Optional[str],
    organisation: Optional[str],
    confidence: float,
    visibility: str,
) -> None:
    async with driver.session() as session:
        r = await session.run(
            """
            MERGE (pr:Project {project_id: $project_id})
            SET pr.name         = $name,
                pr.description  = $description,
                pr.organisation = $organisation,
                pr.confidence   = $confidence,
                pr.visibility   = $visibility,
                pr.updated_at   = $now
            """,
            project_id=project_id, name=name, description=description,
            organisation=organisation, confidence=confidence,
            visibility=visibility, now=_now(),
        )
        await r.consume()
        r = await session.run(
            """
            MATCH (p:Person  {person_id:  $person_id})
            MATCH (pr:Project {project_id: $project_id})
            MERGE (p)-[rel:WORKED_ON]->(pr)
            SET rel.role = $role
            """,
            person_id=person_id, project_id=project_id, role=role,
        )
        await r.consume()


async def upsert_location(
    driver: AsyncDriver,
    *,
    person_id: str,
    site: str,
    floor: Optional[str] = None,
    city: Optional[str] = None,
) -> None:
    location_id = f"loc_{_to_canonical_id(site)}"
    async with driver.session() as session:
        r = await session.run(
            """
            MERGE (loc:Location {location_id: $location_id})
            SET loc.site  = $site,
                loc.floor = $floor,
                loc.city  = $city
            """,
            location_id=location_id, site=site, floor=floor, city=city,
        )
        await r.consume()
        r = await session.run(
            """
            MATCH (p:Person   {person_id:   $person_id})
            MATCH (loc:Location {location_id: $location_id})
            MERGE (p)-[:LOCATED_IN]->(loc)
            """,
            person_id=person_id, location_id=location_id,
        )
        await r.consume()


async def upsert_constraint(
    driver: AsyncDriver,
    *,
    person_id: str,
    constraint_id: str,
    text: str,
    constraint_type: str,
    visibility: str,
) -> None:
    async with driver.session() as session:
        r = await session.run(
            """
            MERGE (c:Constraint {constraint_id: $constraint_id})
            SET c.text            = $text,
                c.constraint_type = $constraint_type,
                c.visibility      = $visibility,
                c.updated_at      = $now
            """,
            constraint_id=constraint_id, text=text,
            constraint_type=constraint_type, visibility=visibility, now=_now(),
        )
        await r.consume()
        r = await session.run(
            """
            MATCH (p:Person     {person_id:     $person_id})
            MATCH (c:Constraint {constraint_id: $constraint_id})
            MERGE (p)-[:HAS_CONSTRAINT]->(c)
            """,
            person_id=person_id, constraint_id=constraint_id,
        )
        await r.consume()


def _to_canonical_id(s: str) -> str:
    import re
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s_]", "", s)
    s = re.sub(r"\s+", "_", s)
    return re.sub(r"_+", "_", s).strip("_") or "unknown"

async def upsert_live_intent(
    driver: AsyncDriver,
    *,
    person_id: str,
    tenant_id: str,
    signal_id: str,
    intent_text: str,
    valid_to: Optional[str] = None,
) -> None:
    intent_id = f"intent_{signal_id}"
    async with driver.session() as session:
        # Expire previous active intents for this person
        r = await session.run(
            """
            MATCH (p:Person {person_id: $person_id})-[:HAS_LIVE_INTENT]->(li:LiveIntent)
            WHERE li.valid_to IS NULL
            SET li.valid_to = $now
            """,
            person_id=person_id, now=_now(),
        )
        await r.consume()

        r = await session.run(
            """
            MERGE (li:LiveIntent {intent_id: $intent_id})
            SET li.tenant_id  = $tenant_id,
                li.text       = $text,
                li.valid_to   = $valid_to,
                li.created_at = $now
            """,
            intent_id=intent_id, tenant_id=tenant_id, text=intent_text,
            valid_to=valid_to, now=_now(),
        )
        await r.consume()

        r = await session.run(
            """
            MATCH (p:Person      {person_id: $person_id})
            MATCH (li:LiveIntent {intent_id: $intent_id})
            MERGE (p)-[:HAS_LIVE_INTENT]->(li)
            """,
            person_id=person_id, intent_id=intent_id,
        )
        await r.consume()


async def upsert_presence(
    driver: AsyncDriver,
    *,
    person_id: str,
    tenant_id: str,
    signal_id: str,
    location: str,
    floor: Optional[str] = None,
    valid_to: Optional[str] = None,
) -> None:
    presence_id = f"presence_{signal_id}"
    async with driver.session() as session:
        r = await session.run(
            """
            MERGE (pr:Presence {presence_id: $presence_id})
            SET pr.tenant_id  = $tenant_id,
                pr.location   = $location,
                pr.floor      = $floor,
                pr.valid_to   = $valid_to,
                pr.last_seen  = $now
            """,
            presence_id=presence_id, tenant_id=tenant_id, location=location,
            floor=floor, valid_to=valid_to, now=_now(),
        )
        await r.consume()

        r = await session.run(
            """
            MATCH (p:Person    {person_id:   $person_id})
            MATCH (pr:Presence {presence_id: $presence_id})
            MERGE (p)-[:PRESENT_AT]->(pr)
            """,
            person_id=person_id, presence_id=presence_id,
        )
        await r.consume()


# ─────────────────────────────────────────────
#  oKG Primitives
# ─────────────────────────────────────────────

async def upsert_match_recommendation(
    driver: AsyncDriver,
    *,
    match_id: str,
    person_a_id: str,
    person_b_id: str,
    score: float,
    transaction_type: str,
    status: str = "recommended",
) -> None:
    async with driver.session() as session:
        r = await session.run(
            """
            MERGE (mr:MatchRecommendation {match_id: $match_id})
            SET mr.score            = $score,
                mr.status           = $status,
                mr.transaction_type = $transaction_type,
                mr.score_version    = 'v1.0',
                mr.created_at       = $now
            """,
            match_id=match_id, score=score, status=status,
            transaction_type=transaction_type, now=_now(),
        )
        await r.consume()

        for pid in [person_a_id, person_b_id]:
            r = await session.run(
                """
                MATCH (p:Person               {person_id: $person_id})
                MATCH (mr:MatchRecommendation {match_id:  $match_id})
                MERGE (p)-[:MATCHED_WITH]->(mr)
                """,
                person_id=pid, match_id=match_id,
            )
            await r.consume()

        # Wire to gKG TransactionType if it exists
        r = await session.run(
            """
            MATCH (mr:MatchRecommendation {match_id: $match_id})
            MATCH (tt:TransactionType)
            WHERE tt.type_id = 'tt_' + $tt OR tt.name = $tt
            MERGE (mr)-[:OF_TYPE]->(tt)
            """,
            match_id=match_id, tt=transaction_type,
        )
        await r.consume()


async def update_match_status(
    driver: AsyncDriver,
    *,
    match_id: str,
    status: str,
) -> None:
    async with driver.session() as session:
        r = await session.run(
            """
            MATCH (mr:MatchRecommendation {match_id: $match_id})
            SET mr.status     = $status,
                mr.updated_at = $now
            """,
            match_id=match_id, status=status, now=_now(),
        )
        await r.consume()


async def upsert_interaction_outcome(
    driver: AsyncDriver,
    *,
    match_id: str,
    outcome_id: str,
    outcome_type: str,
    quality_score: Optional[float] = None,
) -> None:
    async with driver.session() as session:
        r = await session.run(
            """
            MERGE (io:InteractionOutcome {outcome_id: $outcome_id})
            SET io.outcome_type  = $outcome_type,
                io.quality_score = $quality_score,
                io.created_at    = $now
            """,
            outcome_id=outcome_id, outcome_type=outcome_type,
            quality_score=quality_score, now=_now(),
        )
        await r.consume()

        r = await session.run(
            """
            MATCH (mr:MatchRecommendation {match_id:   $match_id})
            MATCH (io:InteractionOutcome  {outcome_id: $outcome_id})
            MERGE (mr)-[:LED_TO]->(io)
            """,
            match_id=match_id, outcome_id=outcome_id,
        )
        await r.consume()


# ─────────────────────────────────────────────
#  High-level orchestrator: iKG from extraction
# ─────────────────────────────────────────────

async def write_extraction_to_ikg(
    driver: AsyncDriver,
    *,
    result,          # ExtractionResult — avoids circular import
    person_id: str,
    tenant_id: str,
    display_name: str,
    headline: str,
    document_id: str,
) -> tuple[int, list[str]]:
    """
    Mirror a completed extraction result into Memgraph iKG.
    Called after write_facts_to_db() succeeds in extraction.py.
    Returns (nodes_written, errors).
    Graph write failure does NOT fail the extraction — errors are logged and returned.
    """
    written = 0
    errors: list[str] = []
    today = _today()

    try:
        await upsert_person(
            driver, person_id=person_id, tenant_id=tenant_id,
            display_name=display_name, headline=headline,
        )
        written += 1
    except Exception as e:
        errors.append(f"iKG Person node: {type(e).__name__}: {e}")
        logger.error(f"iKG Person write failed for {person_id}: {e}")
        # If person node fails, no point continuing
        return written, errors

    for skill in result.skills:
        try:
            ev_id = _make_id()
            await upsert_evidence(
                driver, evidence_id=ev_id, claim_type="skill",
                source_document_id=document_id, confidence=skill.confidence,
                freshness_date=today, visibility=skill.visibility,
            )
            await upsert_skill(
                driver, person_id=person_id,
                skill_name=skill.name,
                canonical_name=skill.canonical_name or skill.name,
                confidence=skill.confidence, visibility=skill.visibility,
                evidence_id=ev_id,
            )
            written += 1
        except Exception as e:
            errors.append(f"iKG Skill '{skill.name}': {type(e).__name__}: {e}")

    for domain in result.domains:
        try:
            ev_id = _make_id()
            await upsert_evidence(
                driver, evidence_id=ev_id, claim_type="domain",
                source_document_id=document_id, confidence=domain.confidence,
                freshness_date=today, visibility=domain.visibility,
            )
            await upsert_domain(
                driver, person_id=person_id,
                domain_name=domain.name,
                canonical_name=domain.canonical_name or domain.name,
                confidence=domain.confidence, visibility=domain.visibility,
                evidence_id=ev_id,
            )
            written += 1
        except Exception as e:
            errors.append(f"iKG Domain '{domain.name}': {type(e).__name__}: {e}")

    for topic in result.topics:
        try:
            await upsert_topic(
                driver, person_id=person_id,
                topic_name=topic.name,
                canonical_name=topic.canonical_name or topic.name,
                confidence=topic.confidence, visibility=topic.visibility,
            )
            written += 1
        except Exception as e:
            errors.append(f"iKG Topic '{topic.name}': {type(e).__name__}: {e}")

    for need in result.needs:
        try:
            need_id = f"need_{person_id}_{_make_id()[:8]}"
            await upsert_need(
                driver, person_id=person_id, need_id=need_id,
                text=need.text, urgency=need.urgency, visibility=need.visibility,
            )
            written += 1
        except Exception as e:
            errors.append(f"iKG Need: {type(e).__name__}: {e}")

    for obj in result.objectives:
        try:
            obj_id = f"obj_{person_id}_{_make_id()[:8]}"
            await upsert_objective(
                driver, person_id=person_id, objective_id=obj_id,
                text=obj.text, urgency=obj.urgency, visibility=obj.visibility,
            )
            written += 1
        except Exception as e:
            errors.append(f"iKG Objective: {type(e).__name__}: {e}")

    for offer in result.offers:
        try:
            offer_id = f"offer_{person_id}_{_make_id()[:8]}"
            await upsert_offer(
                driver, person_id=person_id, offer_id=offer_id,
                text=offer.text, confidence=offer.confidence, visibility=offer.visibility,
            )
            written += 1
        except Exception as e:
            errors.append(f"iKG Offer: {type(e).__name__}: {e}")

    for ach in result.achievements:
        try:
            ach_id = f"ach_{person_id}_{_make_id()[:8]}"
            await upsert_achievement(
                driver, person_id=person_id, achievement_id=ach_id,
                text=ach.text, confidence=ach.confidence, visibility=ach.visibility,
            )
            written += 1
        except Exception as e:
            errors.append(f"iKG Achievement: {type(e).__name__}: {e}")

    for asset in result.assets:
        try:
            asset_id = f"asset_{person_id}_{_make_id()[:8]}"
            await upsert_asset(
                driver, person_id=person_id, asset_id=asset_id,
                name=asset.name, asset_type=asset.asset_type,
                description=asset.description, url_or_ref=asset.url_or_ref,
                confidence=asset.confidence, visibility=asset.visibility,
            )
            written += 1
        except Exception as e:
            errors.append(f"iKG Asset '{asset.name}': {type(e).__name__}: {e}")

    for proj in result.projects:
        try:
            proj_id = f"proj_{person_id}_{_make_id()[:8]}"
            await upsert_project(
                driver, person_id=person_id, project_id=proj_id,
                name=proj.name, description=proj.description,
                role=proj.role, organisation=proj.organisation,
                confidence=proj.confidence, visibility=proj.visibility,
            )
            written += 1
        except Exception as e:
            errors.append(f"iKG Project '{proj.name}': {type(e).__name__}: {e}")

    for loc in result.locations:
        try:
            await upsert_location(
                driver, person_id=person_id,
                site=loc.site, floor=loc.floor, city=loc.city,
            )
            written += 1
        except Exception as e:
            errors.append(f"iKG Location '{loc.site}': {type(e).__name__}: {e}")

    for con in result.constraints:
        try:
            con_id = f"con_{person_id}_{_make_id()[:8]}"
            await upsert_constraint(
                driver, person_id=person_id, constraint_id=con_id,
                text=con.text, constraint_type=con.constraint_type,
                visibility=con.visibility,
            )
            written += 1
        except Exception as e:
            errors.append(f"iKG Constraint: {type(e).__name__}: {e}")

    if errors:
        logger.warning(f"iKG write for {person_id}: {written} nodes, {len(errors)} errors: {errors}")
    else:
        logger.info(f"iKG write for {person_id}: {written} nodes written cleanly")

    return written, errors


# ─────────────────────────────────────────────
#  Generic fact dispatcher
#  Called from profiles.py → update_profile()
#  Routes any fact_type to the correct typed upsert above.
# ─────────────────────────────────────────────

async def upsert_fact_node(
    driver: AsyncDriver,
    *,
    person_id: str,
    tenant_id: str,
    fact_type: str,
    canonical: str,
    raw: str,
    confidence: float,
    visibility: str = "match_engine_only",
) -> None:
    """
    Routes a (fact_type, canonical, raw, confidence) tuple to the correct
    typed iKG upsert function. Unknown types are skipped — never raises,
    so one bad fact never blocks the rest of a profile update.

    Supported fact_types:
        skill | domain | topic | need | objective | offer | achievement | location
    """
    try:
        if fact_type == "skill":
            await upsert_skill(
                driver, person_id=person_id,
                skill_name=raw, canonical_name=canonical,
                confidence=confidence, visibility=visibility,
            )

        elif fact_type == "domain":
            await upsert_domain(
                driver, person_id=person_id,
                domain_name=raw, canonical_name=canonical,
                confidence=confidence, visibility=visibility,
            )

        elif fact_type == "topic":
            await upsert_topic(
                driver, person_id=person_id,
                topic_name=raw, canonical_name=canonical,
                confidence=confidence, visibility=visibility,
            )

        elif fact_type == "need":
            need_id = f"need_{person_id}_{canonical[:30]}"
            await upsert_need(
                driver, person_id=person_id,
                need_id=need_id, text=raw,
                urgency="medium", visibility=visibility,
            )

        elif fact_type == "objective":
            obj_id = f"obj_{person_id}_{canonical[:30]}"
            await upsert_objective(
                driver, person_id=person_id,
                objective_id=obj_id, text=raw,
                urgency="medium", visibility=visibility,
            )

        elif fact_type == "offer":
            offer_id = f"offer_{person_id}_{canonical[:30]}"
            await upsert_offer(
                driver, person_id=person_id,
                offer_id=offer_id, text=raw,
                confidence=confidence, visibility=visibility,
            )

        elif fact_type == "achievement":
            ach_id = f"ach_{person_id}_{canonical[:30]}"
            await upsert_achievement(
                driver, person_id=person_id,
                achievement_id=ach_id, text=raw,
                confidence=confidence, visibility=visibility,
            )

        elif fact_type == "location":
            parts = raw.replace(",", " ").split()
            city = parts[-1] if parts else raw
            await upsert_location(
                driver, person_id=person_id,
                site=raw, floor=None, city=city,
            )

        else:
            logger.debug(
                f"upsert_fact_node: unknown fact_type '{fact_type}' "
                f"for person {person_id[:8]} — skipping"
            )

    except Exception as e:
        logger.warning(
            f"upsert_fact_node failed  person={person_id[:8]} "
            f"type={fact_type} canonical={canonical}: {type(e).__name__}: {e}"
        )