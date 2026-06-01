"""
Delllo RAIN3.0 — Admin Router

POST /v1/tenants             Create a new tenant
POST /v1/users               Create or upsert a user
GET  /v1/users               List all users in a network  (query: ?network_id=...)
PATCH /v1/users/{user_id}/status   Activate / deactivate a single user
POST /v1/users/bulk-status   Activate / deactivate many users at once
POST /v1/admin/wipe          Wipe all tenant data (Postgres + Memgraph)

CHANGES (org/network migration)
────────────────────────────────
• UserCreate: tenant_id REMOVED — user email is now globally unique.
• POST /v1/users: no longer writes tenant_id to users table;
  returns network_suggestions (networks eligible via email domain).
• GET  /v1/users: ?tenant_id replaced by ?network_id — queries
  user_tenants JOIN users for active members of that network.
• POST /v1/users/bulk-status: no longer filters by tenant_id on users
  table (column removed); filters via user_tenants instead.
• POST /v1/admin/wipe: user_profiles wipe no longer sub-selects by
  tenant_id on users; wipes all profiles for users in the network via
  user_tenants.

ID FORMAT NOTE
──────────────
MongoDB ObjectIDs (e.g. "400000000000002000000000") are accepted
everywhere a user_id or tenant_id is expected.
They are stored as-is in TEXT columns and converted to a deterministic
UUID (uuid5 of the raw string) for any column that requires UUID type.
Use mongo_to_uuid() when writing to UUID columns.
"""

import logging
import re
import uuid as _uuid
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db.postgres import get_db
from app.db.graph import get_driver

logger = logging.getLogger(__name__)
router = APIRouter()

# ─────────────────────────────────────────────
#  ID helpers — accept UUID or MongoDB ObjectID
# ─────────────────────────────────────────────

_UUID_RE  = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I
)
_MONGO_RE = re.compile(r'^[0-9a-f]{24}$', re.I)

# Namespace for deterministic UUID generation from MongoDB IDs
_MONGO_NS = _uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


def is_valid_id(val: str) -> bool:
    """Accept standard UUID or 24-char MongoDB ObjectID."""
    return bool(_UUID_RE.match(val) or _MONGO_RE.match(val))


def mongo_to_uuid(val: str) -> str:
    """
    Convert a MongoDB ObjectID to a deterministic UUID-v5.
    Standard UUIDs pass through unchanged.
    Same input always produces the same UUID.
    """
    if _UUID_RE.match(val):
        return val
    return str(_uuid.uuid5(_MONGO_NS, val))


def normalise_id(val: Optional[str]) -> Optional[str]:
    """Return a UUID string from either format, or None."""
    if not val:
        return None
    if not is_valid_id(val):
        raise ValueError(f"Invalid ID format: '{val}'. Expected UUID or 24-char MongoDB ObjectID.")
    return mongo_to_uuid(val)


# ─────────────────────────────────────────────
#  Schemas
# ─────────────────────────────────────────────

class TenantCreate(BaseModel):
    """Create a new tenant / network."""
    tenant_id:   Optional[str] = None   # if omitted, auto-generated UUID
    name:        str
    slug:        str
    description: Optional[str] = None
    config:      Optional[dict] = None

    @field_validator("tenant_id", mode="before")
    @classmethod
    def validate_tenant_id(cls, v):
        if v is None or v == "":
            return None
        if not is_valid_id(v):
            raise ValueError(f"Invalid tenant_id format: '{v}'")
        return mongo_to_uuid(v)


class UserCreate(BaseModel):
    """
    Create or upsert a user.

    CHANGED: tenant_id removed — users are now globally unique by email.
    The caller should follow up with POST /v1/users/{user_id}/join to
    enrol the user in a specific network. The response includes
    network_suggestions so the client can do this in one round-trip.
    """
    user_id:      Optional[str] = None
    display_name: str
    email:        str
    headline:     str = ""
    status:       str = "active"
    # Location fields from Node's user schema
    latitude:     Optional[str] = None
    longitude:    Optional[str] = None
    address:      Optional[str] = None

    @field_validator("user_id", mode="before")
    @classmethod
    def validate_user_id(cls, v):
        if v is None or v == "":
            return None
        if not is_valid_id(v):
            raise ValueError(f"Invalid user_id: '{v}'")
        return mongo_to_uuid(v)


class UserStatusUpdate(BaseModel):
    status: str   # active | disabled
    reason: Optional[str] = None

    @field_validator("status")
    @classmethod
    def check_status(cls, v):
        if v not in ("active", "disabled"):
            raise ValueError("status must be 'active' or 'disabled'")
        return v


class BulkStatusRequest(BaseModel):
    """Activate or deactivate a list of users in one call."""
    network_id: str      # replaces tenant_id — used to validate membership
    user_ids:   List[str]
    status:     str      # active | disabled

    @field_validator("network_id", mode="before")
    @classmethod
    def validate_nid(cls, v):
        if not is_valid_id(str(v)):
            raise ValueError(f"Invalid network_id: '{v}'")
        return mongo_to_uuid(str(v))

    @field_validator("user_ids", mode="before")
    @classmethod
    def validate_uids(cls, v):
        result = []
        for uid in v:
            if not is_valid_id(str(uid)):
                raise ValueError(f"Invalid user_id in list: '{uid}'")
            result.append(mongo_to_uuid(str(uid)))
        return result

    @field_validator("status")
    @classmethod
    def check_status(cls, v):
        if v not in ("active", "disabled"):
            raise ValueError("status must be 'active' or 'disabled'")
        return v


class WipeRequest(BaseModel):
    tenant_id: str
    confirm:   bool = False


# ─────────────────────────────────────────────
#  POST /v1/tenants  — create a new tenant
# ─────────────────────────────────────────────

@router.post("/tenants", status_code=201, summary="Create a new tenant / network")
async def create_tenant(req: TenantCreate, db: AsyncSession = Depends(get_db)):
    """
    Creates a new tenant. Call this once per network in Node.
    Returns the tenant_id to use in all subsequent calls.
    """
    tid  = req.tenant_id or str(_uuid.uuid4())
    slug = req.slug.lower().replace(" ", "-")

    existing = await db.execute(
        text("SELECT tenant_id FROM tenants WHERE slug = :slug"), {"slug": slug}
    )
    if existing.mappings().first():
        raise HTTPException(
            status_code=409,
            detail=f"A tenant with slug '{slug}' already exists."
        )

    import json
    await db.execute(
        text("""
            INSERT INTO tenants (tenant_id, name, slug, status, config_json)
            VALUES (:tid, :name, :slug, 'active', CAST(:config AS JSONB))
            ON CONFLICT (tenant_id) DO UPDATE
                SET name        = EXCLUDED.name,
                    slug        = EXCLUDED.slug,
                    config_json = EXCLUDED.config_json,
                    updated_at  = NOW()
        """),
        {
            "tid":    tid,
            "name":   req.name,
            "slug":   slug,
            "config": json.dumps(req.config or {}),
        },
    )
    await db.commit()

    logger.info(f"Tenant created: {tid[:8]} slug={slug}")
    return {
        "tenant_id":   tid,
        "name":        req.name,
        "slug":        slug,
        "status":      "active",
    }


# ─────────────────────────────────────────────
#  POST /v1/users  — create or upsert a user
# ─────────────────────────────────────────────

@router.post("/users", status_code=201)
async def create_user(req: UserCreate, db: AsyncSession = Depends(get_db)):
    """
    Create or upsert a user + profile.

    CHANGED: tenant_id removed from schema and INSERT — email is now
    globally unique. After creating the user, the response includes
    network_suggestions so the caller can immediately enrol the user
    in eligible networks via POST /v1/users/{user_id}/join.
    """
    uid = req.user_id or str(_uuid.uuid4())

    # Upsert user — no tenant_id column
    await db.execute(
        text("""
            INSERT INTO users
                (user_id, display_name, email, status)
            VALUES
                (:uid, :name, :email, :status)
            ON CONFLICT (user_id) DO UPDATE
                SET display_name = EXCLUDED.display_name,
                    email        = EXCLUDED.email,
                    status       = EXCLUDED.status,
                    updated_at   = NOW()
        """),
        {
            "uid":    uid,
            "name":   req.display_name,
            "email":  req.email,
            "status": req.status,
        },
    )

    # Upsert profile — include location if provided
    location = req.address or (
        f"{req.latitude},{req.longitude}"
        if req.latitude and req.longitude else None
    )
    await db.execute(
        text("""
            INSERT INTO user_profiles
                (user_id, headline, home_location, default_visibility)
            VALUES
                (:uid, :headline, :location, 'match_engine_only')
            ON CONFLICT (user_id) DO UPDATE
                SET headline      = EXCLUDED.headline,
                    home_location = COALESCE(EXCLUDED.home_location, user_profiles.home_location)
        """),
        {"uid": uid, "headline": req.headline, "location": location},
    )
    await db.commit()

    # ── Network suggestions based on email domain ─────────────
    # Find networks with an email_domain or open join rule that
    # matches the user's email domain, where they are not yet a member.
    email_domain = req.email.split("@")[-1].lower() if "@" in req.email else ""
    suggestions_result = await db.execute(
        text("""
            SELECT DISTINCT
                t.tenant_id,
                t.name   AS network_name,
                t.slug   AS network_slug,
                r.rule_type
            FROM network_join_rules r
            JOIN tenants t ON t.tenant_id = r.tenant_id
            WHERE (
                (r.rule_type = 'email_domain' AND LOWER(r.rule_value) = :domain)
                OR r.rule_type = 'open'
            )
            AND t.tenant_id NOT IN (
                SELECT tenant_id FROM user_tenants WHERE user_id = :uid
            )
            ORDER BY t.name
        """),
        {"domain": email_domain, "uid": uid},
    )
    suggestions = [
        {
            "tenant_id":    str(r["tenant_id"]),
            "network_name": r["network_name"],
            "network_slug": r["network_slug"],
            "rule_type":    r["rule_type"],
        }
        for r in suggestions_result.mappings().all()
    ]

    logger.info(f"User upserted: {uid[:8]} ({req.display_name}), {len(suggestions)} network suggestion(s)")
    return {
        "user_id":             uid,
        "display_name":        req.display_name,
        "status":              "created",
        "network_suggestions": suggestions,
    }


# ─────────────────────────────────────────────
#  GET /v1/users
#  CHANGED: ?tenant_id replaced by ?network_id
#  Queries user_tenants JOIN users for active
#  members of that network.
# ─────────────────────────────────────────────

@router.get("/users")
async def list_users(network_id: str, db: AsyncSession = Depends(get_db)):
    """
    List all active members of a network.

    CHANGED: previously filtered by tenant_id on the users table.
    Now queries user_tenants JOIN users so the result reflects actual
    network membership rather than the (now-removed) tenant_id column.
    """
    nid = mongo_to_uuid(network_id) if is_valid_id(network_id) else network_id

    result = await db.execute(
        text("""
            SELECT
                u.user_id,
                u.display_name,
                u.email,
                ut.role,
                u.status,
                p.headline,
                p.home_location,
                ut.joined_at,
                COUNT(ef.fact_id) AS fact_count
            FROM user_tenants ut
            JOIN users u ON u.user_id = ut.user_id
            LEFT JOIN user_profiles p ON p.user_id = u.user_id
            LEFT JOIN extracted_facts ef
                ON ef.user_id = u.user_id AND ef.tenant_id = :nid
            WHERE ut.tenant_id = :nid
              AND ut.status    = 'active'
            GROUP BY
                u.user_id, u.display_name, u.email, ut.role,
                u.status, p.headline, p.home_location, ut.joined_at
            ORDER BY u.display_name
        """),
        {"nid": nid},
    )
    rows = result.mappings().all()
    return {"network_id": nid, "users": [dict(r) for r in rows], "count": len(rows)}


# ─────────────────────────────────────────────
#  PATCH /v1/users/{user_id}/status
# ─────────────────────────────────────────────

@router.patch("/users/{user_id}/status", summary="Activate or deactivate a user")
async def update_user_status(
    user_id: str,
    req: UserStatusUpdate,
    db: AsyncSession = Depends(get_db),
):
    """
    Node calls this when a user leaves a network (status=disabled)
    or rejoins (status=active). RAIN immediately removes disabled users
    from all future candidate pools.
    """
    uid = mongo_to_uuid(user_id) if is_valid_id(user_id) else user_id

    result = await db.execute(
        text("SELECT user_id FROM users WHERE user_id = :uid"), {"uid": uid}
    )
    if not result.mappings().first():
        raise HTTPException(status_code=404, detail="User not found")

    await db.execute(
        text("""
            UPDATE users
            SET status = :status, updated_at = NOW()
            WHERE user_id = :uid
        """),
        {"uid": uid, "status": req.status},
    )

    # If disabling — expire all active sKG signals immediately
    if req.status == "disabled":
        await db.execute(
            text("""
                UPDATE live_signals
                SET valid_to = NOW()
                WHERE user_id = :uid AND valid_to IS NULL
            """),
            {"uid": uid},
        )

    await db.commit()
    logger.info(f"User {uid[:8]} status → {req.status} (reason: {req.reason})")
    return {"user_id": uid, "status": req.status}


# ─────────────────────────────────────────────
#  POST /v1/users/bulk-status
# ─────────────────────────────────────────────

@router.post("/users/bulk-status", summary="Activate or deactivate multiple users at once")
async def bulk_update_status(req: BulkStatusRequest, db: AsyncSession = Depends(get_db)):
    """
    Efficiently update the status of many users at once.

    CHANGED: no longer filters by tenant_id on the users table (column
    removed). Now validates that every user_id in the list is an active
    member of the specified network via user_tenants before updating.
    """
    if not req.user_ids:
        raise HTTPException(status_code=400, detail="user_ids list is empty")

    nid = req.network_id

    # Validate all user_ids are active members of the network
    membership_result = await db.execute(
        text("""
            SELECT user_id FROM user_tenants
            WHERE tenant_id = :nid
              AND status     = 'active'
              AND user_id    = ANY(:uids)
        """),
        {"nid": nid, "uids": req.user_ids},
    )
    valid_members = {str(r["user_id"]) for r in membership_result.mappings().all()}
    invalid = [uid for uid in req.user_ids if uid not in valid_members]
    if invalid:
        logger.warning(
            f"bulk-status: {len(invalid)} user_id(s) are not active members of "
            f"network {nid[:8]} — they will be skipped: {invalid[:5]}"
        )

    # Only update users confirmed as network members
    member_list = list(valid_members)
    if not member_list:
        return {
            "network_id":    nid,
            "users_updated": 0,
            "status":        req.status,
            "skipped":       len(invalid),
        }

    await db.execute(
        text("""
            UPDATE users
            SET status = :status, updated_at = NOW()
            WHERE user_id = ANY(:uids)
        """),
        {"status": req.status, "uids": member_list},
    )

    # Expire sKG signals for all disabled users
    if req.status == "disabled":
        await db.execute(
            text("""
                UPDATE live_signals
                SET valid_to = NOW()
                WHERE user_id = ANY(:uids) AND valid_to IS NULL
            """),
            {"uids": member_list},
        )

    await db.commit()
    logger.info(
        f"Bulk status update: {len(member_list)} users → {req.status} "
        f"network={nid[:8]} (skipped {len(invalid)} non-members)"
    )
    return {
        "network_id":    nid,
        "users_updated": len(member_list),
        "status":        req.status,
        "skipped":       len(invalid),
    }


# ─────────────────────────────────────────────
#  POST /v1/admin/wipe
# ─────────────────────────────────────────────

@router.post("/admin/wipe")
async def wipe_tenant(req: WipeRequest, db: AsyncSession = Depends(get_db)):
    """Wipe all data for a tenant. Requires confirm=True."""
    if not req.confirm:
        raise HTTPException(status_code=400, detail="Set confirm=true to execute wipe")

    tid = mongo_to_uuid(req.tenant_id) if is_valid_id(req.tenant_id) else req.tenant_id
    wiped_tables = []
    errors       = []

    tables_to_clear = [
        ("feature_snapshots",         "tenant_id"),
        ("notifications",             "tenant_id"),
        ("feedback_events",           None),
        ("match_scores",              None),
        ("explanations",              None),
        ("matches",                   "tenant_id"),
        ("live_signals",              "tenant_id"),
        ("extracted_facts",           "tenant_id"),
        ("document_chunks",           None),
        ("documents",                 "tenant_id"),
        ("audit_log",                 "tenant_id"),
        ("tenant_ontology_overrides", "tenant_id"),
        ("user_profiles",             None),
        ("user_tenants",              "tenant_id"),   # ← remove memberships too
    ]

    for table, tid_col in tables_to_clear:
        try:
            if tid_col:
                await db.execute(
                    text(f"DELETE FROM {table} WHERE {tid_col} = :tid"), {"tid": tid}
                )
            else:
                if table == "document_chunks":
                    await db.execute(text("""
                        DELETE FROM document_chunks WHERE document_id IN (
                            SELECT document_id FROM documents WHERE tenant_id = :tid
                        )"""), {"tid": tid})
                elif table == "feedback_events":
                    await db.execute(text("""
                        DELETE FROM feedback_events WHERE match_id IN (
                            SELECT match_id FROM matches WHERE tenant_id = :tid
                        )"""), {"tid": tid})
                elif table in ("match_scores", "explanations"):
                    await db.execute(text(f"""
                        DELETE FROM {table} WHERE match_id IN (
                            SELECT match_id FROM matches WHERE tenant_id = :tid
                        )"""), {"tid": tid})
                elif table == "user_profiles":
                    # CHANGED: no tenant_id on users; sub-select via user_tenants
                    await db.execute(text("""
                        DELETE FROM user_profiles WHERE user_id IN (
                            SELECT user_id FROM user_tenants WHERE tenant_id = :tid
                        )"""), {"tid": tid})
            wiped_tables.append(table)
        except Exception as e:
            errors.append(f"{table}: {type(e).__name__}: {e}")
            logger.error(f"Wipe error for {table}: {e}")

    memgraph_wiped = False
    try:
        driver = get_driver()
        async with driver.session() as session:
            await session.run("MATCH (p:Person {tenant_id: $tid}) DETACH DELETE p", tid=tid)
            await session.run("MATCH (li:LiveIntent {tenant_id: $tid}) DETACH DELETE li", tid=tid)
            await session.run("MATCH (pr:Presence {tenant_id: $tid}) DETACH DELETE pr", tid=tid)
            await session.run(
                "MATCH (mr:MatchRecommendation) WHERE mr.tenant_id = $tid DETACH DELETE mr", tid=tid
            )
        memgraph_wiped = True
    except Exception as e:
        errors.append(f"Memgraph: {type(e).__name__}: {e}")
    await db.commit()
    return {
        "tenant_id":      tid,
        "tables_wiped":   wiped_tables,
        "memgraph_wiped": memgraph_wiped,
        "errors":         errors,
        "status":         "ok" if not errors else "partial",
    }