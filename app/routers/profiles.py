"""
Delllo RAIN3.0 — Profiles Router

GET  /v1/profiles/{user_id}              Get profile + facts
GET  /v1/profiles/{user_id}/facts        List extracted facts
POST /v1/profiles/{user_id}/update       Rich profile update from Node  ← NEW
PATCH /v1/profiles/{user_id}             Patch simple profile fields

Accepts UUID or MongoDB ObjectID for user_id.
"""

import json
import logging
import uuid as _uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db.postgres import get_db
from app.db.graph import get_driver
from app.services import graph_writer

logger = logging.getLogger(__name__)
router = APIRouter()

# Re-use the ID helpers from admin (same logic, avoids circular import)
import re as _re
_UUID_RE  = _re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', _re.I)
_MONGO_RE = _re.compile(r'^[0-9a-f]{24}$', _re.I)
_MONGO_NS = _uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

def _norm(val: str) -> str:
    if _UUID_RE.match(val): return val
    if _MONGO_RE.match(val): return str(_uuid.uuid5(_MONGO_NS, val))
    raise HTTPException(status_code=400, detail=f"Invalid ID format: '{val}'")


# ─────────────────────────────────────────────
#  Schemas — Node profile format
# ─────────────────────────────────────────────

class CurrentRole(BaseModel):
    title:    str
    company:  str
    location: Optional[str] = None

class PreviousRole(BaseModel):
    title:   str
    company: str
    period:  Optional[str] = None

class Skill(BaseModel):
    skill:      str
    level:      Optional[str] = None       # Beginner | Intermediate | Expert
    applied_in: Optional[str] = None       # context / reference

class UserObjective(BaseModel):
    primary_goal:     str
    secondary_goals:  List[str] = []
    target_profiles:  List[str] = []
    exclude:          List[str] = []
    success_signals:  List[str] = []

class ImmediateNeed(BaseModel):
    text: str

class BusinessDriver(BaseModel):
    service: str

class NodeUserProfile(BaseModel):
    """
    Rich profile payload as sent by Node.
    Maps onto RAIN's extracted_facts + iKG structures.
    """
    current_role:       Optional[CurrentRole]       = None
    previous_roles:     List[PreviousRole]           = []
    top_skills:         List[Skill]                  = []
    solutions_offered:  List[str]                    = []
    career_highlights:  List[str]                    = []
    immediate_needs:    List[str]                    = []   # from ImmediateNeeds array
    business_drivers:   List[str]                    = []   # from businessDrivers
    education:          List[Dict[str, Any]]         = []
    latitude:           Optional[str]                = None
    longitude:          Optional[str]                = None
    address:            Optional[str]                = None

class ProfileUpdateRequest(BaseModel):
    """
    Full profile update request from Node.
    user_profile   — what the person knows / has done
    user_objective — what they want right now
    tenant_id      — required to scope fact writes
    """
    tenant_id:       str
    user_profile:    Optional[NodeUserProfile]  = None
    user_objective:  Optional[UserObjective]    = None


class ProfilePatch(BaseModel):
    headline:    Optional[str] = None
    summary:     Optional[str] = None
    visibility:  Optional[str] = None


# ─────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────

def _slug(text: str) -> str:
    """Canonical snake_case key for a fact value."""
    return _re.sub(r'[^a-z0-9]+', '_', text.lower()).strip('_')[:120]


async def _write_facts(
    db: AsyncSession,
    user_id: str,
    tenant_id: str,
    facts: List[tuple],   # (fact_type, canonical, raw, confidence)
):
    """
    Upsert a list of extracted facts.
    Uses ON CONFLICT DO UPDATE so re-sending the same profile is safe.
    """
    for (ftype, canonical, raw, conf) in facts:
        fact_id = str(_uuid.uuid4())
        await db.execute(
            text("""
                INSERT INTO extracted_facts
                    (fact_id, tenant_id, user_id, fact_type,
                     canonical_value, raw_value, confidence, visibility)
                VALUES (:fid, :tid, :uid, :ftype, :canon, :raw, :conf, 'match_engine_only')
                ON CONFLICT (tenant_id, user_id, fact_type, canonical_value)
                    DO UPDATE SET
                        raw_value  = EXCLUDED.raw_value,
                        confidence = EXCLUDED.confidence
            """),
            {
                "fid":   fact_id, "tid": tenant_id, "uid": user_id,
                "ftype": ftype,   "canon": canonical,
                "raw":   raw,     "conf": conf,
            },
        )


def _profile_to_facts(profile: NodeUserProfile) -> List[tuple]:
    """Convert a NodeUserProfile into (fact_type, canonical, raw, confidence) tuples."""
    facts = []

    # Current role → skill + domain
    if profile.current_role:
        r = profile.current_role
        headline = f"{r.title} at {r.company}"
        facts.append(('skill',  _slug(r.title),   r.title,   0.95))
        facts.append(('domain', _slug(r.company),  r.company, 0.90))
        if r.location:
            facts.append(('location', _slug(r.location), r.location, 0.95))

    # Previous roles → skill + domain (lower confidence)
    for prev in profile.previous_roles:
        facts.append(('skill',  _slug(prev.title),   prev.title,   0.80))
        facts.append(('domain', _slug(prev.company),  prev.company, 0.75))

    # Skills
    for s in profile.top_skills:
        conf = {'beginner': 0.65, 'intermediate': 0.80, 'expert': 0.95}.get(
            (s.level or '').lower(), 0.80
        )
        raw = s.skill + (f" — {s.applied_in}" if s.applied_in else "")
        facts.append(('skill', _slug(s.skill), raw, conf))

    # Solutions offered → offer
    for sol in profile.solutions_offered:
        facts.append(('offer', _slug(sol), sol, 0.85))

    # Career highlights → achievement
    for h in profile.career_highlights:
        facts.append(('achievement', _slug(h[:60]), h, 0.85))

    # Immediate needs → need
    for n in profile.immediate_needs:
        facts.append(('need', _slug(n[:80]), n, 0.90))

    # Business drivers → domain
    for d in profile.business_drivers:
        facts.append(('domain', _slug(d[:80]), d, 0.85))

    # Location from coords
    if profile.address:
        facts.append(('location', _slug(profile.address), profile.address, 0.90))
    elif profile.latitude and profile.longitude:
        loc = f"{profile.latitude},{profile.longitude}"
        facts.append(('location', _slug(loc), loc, 0.70))

    # Deduplicate by (type, canonical)
    seen = set()
    deduped = []
    for f in facts:
        key = (f[0], f[1])
        if key not in seen:
            seen.add(key)
            deduped.append(f)
    return deduped


def _objective_to_facts(obj: UserObjective) -> List[tuple]:
    """Convert a UserObjective into need/objective facts."""
    facts = []
    facts.append(('objective', _slug(obj.primary_goal[:80]), obj.primary_goal, 0.95))
    for sg in obj.secondary_goals:
        facts.append(('need', _slug(sg[:80]), sg, 0.85))
    for sp in obj.success_signals:
        facts.append(('objective', _slug(sp[:80]), sp, 0.75))
    return facts


# ─────────────────────────────────────────────
#  GET /v1/profiles/{user_id}
# ─────────────────────────────────────────────

@router.get("/profiles/{user_id}")
async def get_profile(user_id: str, db: AsyncSession = Depends(get_db)):
    uid = _norm(user_id)
    result = await db.execute(
        text("""
            SELECT u.user_id, u.display_name, u.email, u.role, u.status,
                   p.headline, p.summary, p.home_location, p.default_visibility
            FROM users u
            LEFT JOIN user_profiles p ON p.user_id = u.user_id
            WHERE u.user_id = :uid
        """),
        {"uid": uid},
    )
    row = result.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    return dict(row)


# ─────────────────────────────────────────────
#  GET /v1/profiles/{user_id}/facts
# ─────────────────────────────────────────────

@router.get("/profiles/{user_id}/facts")
async def get_profile_facts(user_id: str, db: AsyncSession = Depends(get_db)):
    uid = _norm(user_id)
    result = await db.execute(
        text("""
            SELECT fact_id, fact_type, canonical_value, raw_value,
                   confidence, visibility, validated_by_user, freshness_date
            FROM extracted_facts
            WHERE user_id = :uid
            ORDER BY confidence DESC
        """),
        {"uid": uid},
    )
    rows = result.mappings().all()
    return {"user_id": uid, "facts": [dict(r) for r in rows], "count": len(rows)}


# ─────────────────────────────────────────────
#  POST /v1/profiles/{user_id}/update  ← NEW
#  Rich profile update from Node
# ─────────────────────────────────────────────

@router.post("/profiles/{user_id}/update", summary="Update user profile from Node")
async def update_profile(
    user_id: str,
    req: ProfileUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Main profile ingestion endpoint for Node.

    Accepts Node's rich profile format (current_role, skills, solutions,
    highlights, immediate_needs, business_drivers, objectives) and:
      1. Updates user_profiles (headline, location)
      2. Writes all fields as extracted_facts (upsert — safe to call repeatedly)
      3. Re-syncs the user's iKG in Memgraph from the new facts
      4. If user_objective present, posts it as a live intent signal

    Returns fact counts by type.
    """
    uid = _norm(user_id)
    tid = _norm(req.tenant_id) if _re.match(r'^[0-9a-f]{24}$', req.tenant_id, _re.I) \
          else req.tenant_id

    # Verify user exists
    exists = await db.execute(
        text("SELECT user_id FROM users WHERE user_id = :uid"), {"uid": uid}
    )
    if not exists.mappings().first():
        raise HTTPException(status_code=404, detail="User not found. Create the user first.")

    all_facts: List[tuple] = []

    # ── Profile facts ─────────────────────────────────────────
    if req.user_profile:
        all_facts.extend(_profile_to_facts(req.user_profile))

        # Update headline + location in user_profiles
        p = req.user_profile
        headline = None
        if p.current_role:
            headline = f"{p.current_role.title} at {p.current_role.company}"
        location = p.address or (
            f"{p.latitude},{p.longitude}" if p.latitude and p.longitude else None
        )
        if headline or location:
            await db.execute(
                text("""
                    UPDATE user_profiles
                    SET headline      = COALESCE(:headline, headline),
                        home_location = COALESCE(:location, home_location)
                    WHERE user_id = :uid
                """),
                {"uid": uid, "headline": headline, "location": location},
            )

    # ── Objective facts ───────────────────────────────────────
    if req.user_objective:
        all_facts.extend(_objective_to_facts(req.user_objective))

        # Also post the primary goal as a live intent signal so sKG is current
        intent_text = req.user_objective.primary_goal
        if intent_text:
            # Expire old intents first
            await db.execute(
                text("""
                    UPDATE live_signals
                    SET valid_to = NOW()
                    WHERE user_id = :uid AND signal_type = 'intent' AND valid_to IS NULL
                """),
                {"uid": uid},
            )
            signal_result = await db.execute(
                text("""
                    INSERT INTO live_signals
                        (tenant_id, user_id, signal_type, payload_json)
                    VALUES (:tid, :uid, 'intent', CAST(:payload AS JSONB))
                    RETURNING signal_id
                """),
                {
                    "tid": tid, "uid": uid,
                    "payload": json.dumps({
                        "text":    intent_text,
                        "urgency": "medium",
                        "source":  "profile_update",
                    }),
                },
            )
            signal_id = str(signal_result.mappings().first()["signal_id"])
            # Mirror to Memgraph sKG (non-fatal)
            try:
                await graph_writer.upsert_live_intent(
                    get_driver(),
                    person_id=uid, tenant_id=tid,
                    signal_id=signal_id, intent_text=intent_text,
                    valid_to=None,
                )
            except Exception as e:
                logger.warning(f"sKG intent write failed (non-fatal): {e}")

    # ── Write facts to Postgres ───────────────────────────────
    if all_facts:
        await _write_facts(db, uid, tid, all_facts)

    await db.commit()

    # ── Sync iKG in Memgraph ──────────────────────────────────
    ikg_synced = False
    ikg_error  = None
    if all_facts:
        try:
            # Load profile name for iKG Person node
            name_row = await db.execute(
                text("SELECT display_name FROM users WHERE user_id = :uid"), {"uid": uid}
            )
            display_name = (name_row.mappings().first() or {}).get("display_name", "")

            await graph_writer.upsert_person(
                get_driver(), person_id=uid, tenant_id=tid,
                display_name=display_name, headline=""
            )
            for (ftype, canonical, raw, conf) in all_facts:
                await graph_writer.upsert_fact_node(
                    get_driver(), person_id=uid, tenant_id=tid,
                    fact_type=ftype, canonical=canonical, raw=raw, confidence=conf,
                )
            ikg_synced = True
        except Exception as e:
            ikg_error = str(e)
            logger.warning(f"iKG sync failed (non-fatal): {e}")

    # ── Summarise by fact type ────────────────────────────────
    type_counts: Dict[str, int] = {}
    for (ftype, _, _, _) in all_facts:
        type_counts[ftype] = type_counts.get(ftype, 0) + 1

    return {
        "user_id":     uid,
        "tenant_id":   tid,
        "facts_written": len(all_facts),
        "fact_breakdown": type_counts,
        "ikg_synced":  ikg_synced,
        "ikg_error":   ikg_error,
        "intent_posted": req.user_objective is not None,
    }


# ─────────────────────────────────────────────
#  PATCH /v1/profiles/{user_id}
#  Simple field patch (headline, summary, visibility)
# ─────────────────────────────────────────────

@router.patch("/profiles/{user_id}")
async def patch_profile(
    user_id: str,
    req: ProfilePatch,
    db: AsyncSession = Depends(get_db),
):
    uid = _norm(user_id)
    updates = {}
    if req.headline   is not None: updates["headline"]           = req.headline
    if req.summary    is not None: updates["summary"]            = req.summary
    if req.visibility is not None: updates["default_visibility"] = req.visibility

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    await db.execute(
        text(f"UPDATE user_profiles SET {set_clause} WHERE user_id = :uid"),
        {"uid": uid, **updates},
    )
    await db.commit()
    return {"user_id": uid, "updated": list(updates.keys())}