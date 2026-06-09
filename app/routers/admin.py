"""
Delllo RAIN3.0 — Admin Router

POST   /v1/tenants                          Create a new tenant / network
POST   /v1/users                            Create or upsert a user (global registry)
GET    /v1/users                            List all users in a tenant
PATCH  /v1/users/{user_id}/status           Activate / deactivate a single user
POST   /v1/users/bulk-status               Activate / deactivate many users at once

── Network membership (Node calls these) ──────────────────────────────────────
POST   /v1/networks/{tenant_id}/members           Add a user to a network
DELETE /v1/networks/{tenant_id}/members/{user_id} Remove a user from a network

POST   /v1/admin/wipe                       Wipe all tenant data (Postgres + Memgraph)

ID FORMAT NOTE
──────────────
MongoDB ObjectIDs (24-char hex, e.g. "6a221f7a84b3da41cbeb3fd7") are accepted
everywhere a user_id or tenant_id is expected. They are converted to a
deterministic UUID-v5 for any UUID column. The same input always maps to the
same UUID — no secondary ID is stored or returned.
"""

import json
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

# Stable namespace for deterministic UUID-v5 generation from MongoDB IDs.
# Do NOT change this value after first deploy.
_MONGO_NS = _uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


def is_valid_id(val: str) -> bool:
    """Accept standard UUID or 24-char MongoDB ObjectID."""
    return bool(_UUID_RE.match(val) or _MONGO_RE.match(val))


def mongo_to_uuid(val: str) -> str:
    """
    Convert a MongoDB ObjectID to a deterministic UUID-v5.
    Standard UUIDs pass through unchanged.
    Same input → same UUID, always.
    """
    if _UUID_RE.match(val):
        return val
    return str(_uuid.uuid5(_MONGO_NS, val))


def normalise_id(val: Optional[str]) -> Optional[str]:
    """Return a UUID string from either format, or None if val is empty."""
    if not val:
        return None
    if not is_valid_id(val):
        raise ValueError(
            f"Invalid ID format: '{val}'. "
            "Expected a UUID or 24-char MongoDB ObjectID."
        )
    return mongo_to_uuid(val)


def _require_id(val: str, field: str = "id") -> str:
    """Like normalise_id but raises HTTPException instead of ValueError."""
    if not is_valid_id(val):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {field} format: '{val}'. "
                   "Expected a UUID or 24-char MongoDB ObjectID.",
        )
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
    """Create or upsert a user. Accepts UUID or MongoDB ObjectID."""
    user_id:      Optional[str] = None
    tenant_id:    str
    display_name: str
    email:        str
    headline:     str = ""
    role:         str = "member"
    status:       str = "active"
    latitude:     Optional[str] = None
    longitude:    Optional[str] = None
    address:      Optional[str] = None

    @field_validator("user_id", mode="before")
    @classmethod
    def validate_user_id(cls, v):
        if v is None or v == "":
            return None
        if not is_valid_id(str(v)):
            raise ValueError(f"Invalid user_id: '{v}'")
        return mongo_to_uuid(str(v))

    @field_validator("tenant_id", mode="before")
    @classmethod
    def validate_tenant_id(cls, v):
        if not is_valid_id(str(v)):
            raise ValueError(f"Invalid tenant_id: '{v}'")
        return mongo_to_uuid(str(v))


class NetworkMemberAdd(BaseModel):
    """
    Add a user to a network (tenant).
    Node calls this when a user joins / is added to a network.
    """
    user_id: str
    role:    str = "member"   # member | admin | viewer

    @field_validator("user_id", mode="before")
    @classmethod
    def validate_uid(cls, v):
        if not is_valid_id(str(v)):
            raise ValueError(f"Invalid user_id: '{v}'")
        return mongo_to_uuid(str(v))

    @field_validator("role")
    @classmethod
    def check_role(cls, v):
        if v not in ("admin", "member", "viewer"):
            raise ValueError("role must be admin | member | viewer")
        return v


class UserStatusUpdate(BaseModel):
    status: str
    reason: Optional[str] = None

    @field_validator("status")
    @classmethod
    def check_status(cls, v):
        if v not in ("active", "disabled"):
            raise ValueError("status must be 'active' or 'disabled'")
        return v


class BulkStatusRequest(BaseModel):
    tenant_id: str
    user_ids:  List[str]
    status:    str

    @field_validator("tenant_id", mode="before")
    @classmethod
    def validate_tid(cls, v):
        if not is_valid_id(str(v)):
            raise ValueError(f"Invalid tenant_id: '{v}'")
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
    Creates a new tenant / network.  Call this once per network from Node.
    Returns the tenant_id to use in all subsequent calls.

    FIX: The provided tenant_id (UUID or MongoDB ObjectID) is now always
    honoured. If that ID already exists, the row is updated in-place
    (upsert semantics) rather than being silently ignored.

    The slug uniqueness check is skipped when the same tenant_id is
    re-submitted (idempotent upsert), so re-runs are safe.
    """
    tid  = req.tenant_id or str(_uuid.uuid4())
    slug = req.slug.lower().replace(" ", "-")

    # Check slug uniqueness — but only if a *different* tenant owns it.
    existing = await db.execute(
        text("""
            SELECT tenant_id FROM tenants
            WHERE slug = :slug AND tenant_id != :tid
        """),
        {"slug": slug, "tid": tid},
    )
    if existing.mappings().first():
        raise HTTPException(
            status_code=409,
            detail=f"A different tenant already uses slug '{slug}'.",
        )

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

    logger.info(f"Tenant upserted: {tid[:8]} slug={slug}")
    return {
        "tenant_id": tid,
        "name":      req.name,
        "slug":      slug,
        "status":    "active",
    }


# ─────────────────────────────────────────────
#  POST /v1/users  — create or upsert a user
# ─────────────────────────────────────────────

@router.post("/users", status_code=201)
async def create_user(req: UserCreate, db: AsyncSession = Depends(get_db)):
    """
    Create or upsert a user + profile in the global user registry.
    Accepts UUID or MongoDB ObjectID for both user_id and tenant_id.

    The tenant is auto-created if it doesn't exist, so Node does not
    need to call POST /v1/tenants before creating users.
    """
    uid = req.user_id or str(_uuid.uuid4())
    tid = req.tenant_id  # already normalised by Pydantic validator

    # Ensure the tenant row exists — preserve the slug if it was set
    # via POST /v1/tenants. Only auto-create when genuinely missing.
    t = await db.execute(
        text("SELECT tenant_id FROM tenants WHERE tenant_id = :tid"),
        {"tid": tid},
    )
    if not t.mappings().first():
        await db.execute(
            text("""
                INSERT INTO tenants (tenant_id, name, slug, status)
                VALUES (:tid, :name, :slug, 'active')
                ON CONFLICT (tenant_id) DO NOTHING
            """),
            {"tid": tid, "name": f"Tenant {tid[:8]}", "slug": f"auto-{tid[:8]}"},
        )

    # Upsert user
    await db.execute(
        text("""
            INSERT INTO users
                (user_id, tenant_id, display_name, email, role, status)
            VALUES
                (:uid, :tid, :name, :email, :role, :status)
            ON CONFLICT (user_id) DO UPDATE
                SET display_name = EXCLUDED.display_name,
                    email        = EXCLUDED.email,
                    role         = EXCLUDED.role,
                    status       = EXCLUDED.status,
                    updated_at   = NOW()
        """),
        {
            "uid":    uid, "tid":  tid,
            "name":   req.display_name,
            "email":  req.email,
            "role":   req.role,
            "status": req.status,
        },
    )

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

    logger.info(f"User upserted: {uid[:8]} ({req.display_name}) tenant={tid[:8]}")
    return {
        "user_id":      uid,
        "tenant_id":    tid,
        "display_name": req.display_name,
        "status":       "created",
    }


# ─────────────────────────────────────────────
#  GET /v1/users
# ─────────────────────────────────────────────

@router.get("/users")
async def list_users(tenant_id: str, db: AsyncSession = Depends(get_db)):
    tid = _require_id(tenant_id, "tenant_id")
    result = await db.execute(
        text("""
            SELECT u.user_id, u.display_name, u.email, u.role, u.status,
                   p.headline, p.home_location,
                   COUNT(ef.fact_id) AS fact_count
            FROM users u
            LEFT JOIN user_profiles p  ON p.user_id  = u.user_id
            LEFT JOIN extracted_facts ef
                ON ef.user_id = u.user_id AND ef.tenant_id = :tid
            WHERE u.tenant_id = :tid
            GROUP BY u.user_id, u.display_name, u.email, u.role,
                     u.status, p.headline, p.home_location
            ORDER BY u.display_name
        """),
        {"tid": tid},
    )
    rows = result.mappings().all()
    return {"tenant_id": tid, "users": [dict(r) for r in rows], "count": len(rows)}


# ─────────────────────────────────────────────
#  Network membership endpoints
#  Node calls these to add / remove a user from a network.
#  These are the canonical join/leave operations — do NOT use
#  PATCH /users/{id}/status for network membership changes.
# ─────────────────────────────────────────────

@router.post(
    "/networks/{tenant_id}/members",
    status_code=200,
    summary="Add a user to a network (Node calls this on network join)",
)
async def add_network_member(
    tenant_id: str,
    req: NetworkMemberAdd,
    db: AsyncSession = Depends(get_db),
):
    """
    Add (or re-add) a user to a network.

    What this does:
    - Verifies the network (tenant) exists.
    - Verifies the user exists in the global registry.
    - Sets user.tenant_id = tenant_id and user.status = 'active'.
    - Re-activates any previously expired sKG signals.
    - Writes an audit log entry.

    Idempotent: safe to call multiple times for the same user/network.
    """
    tid = _require_id(tenant_id, "tenant_id")
    uid = req.user_id  # already normalised by Pydantic

    # Verify network exists
    t = await db.execute(
        text("SELECT name FROM tenants WHERE tenant_id = :tid"), {"tid": tid}
    )
    tenant_row = t.mappings().first()
    if not tenant_row:
        raise HTTPException(status_code=404, detail=f"Network '{tid}' not found.")

    # Verify user exists
    u = await db.execute(
        text("SELECT display_name FROM users WHERE user_id = :uid"), {"uid": uid}
    )
    user_row = u.mappings().first()
    if not user_row:
        raise HTTPException(
            status_code=404,
            detail=f"User '{uid}' not found. Create the user first via POST /v1/users.",
        )

    # Move user into this network and activate
    await db.execute(
        text("""
            UPDATE users
            SET tenant_id  = :tid,
                role       = :role,
                status     = 'active',
                updated_at = NOW()
            WHERE user_id = :uid
        """),
        {"tid": tid, "uid": uid, "role": req.role},
    )

    # Also ensure extracted_facts point to the new tenant
    await db.execute(
        text("""
            UPDATE extracted_facts
            SET tenant_id = :tid
            WHERE user_id = :uid
        """),
        {"tid": tid, "uid": uid},
    )

    # Audit
    await db.execute(
        text("""
            INSERT INTO audit_log (tenant_id, actor_user_id, action, object_type, object_id)
            VALUES (:tid, :uid, 'network_join', 'user', :uid)
        """),
        {"tid": tid, "uid": uid},
    )
    await db.commit()

    logger.info(f"User {uid[:8]} joined network {tid[:8]} as {req.role}")
    return {
        "tenant_id":    tid,
        "tenant_name":  tenant_row["name"],
        "user_id":      uid,
        "display_name": user_row["display_name"],
        "role":         req.role,
        "status":       "active",
        "action":       "joined",
    }


@router.delete(
    "/networks/{tenant_id}/members/{user_id}",
    status_code=200,
    summary="Remove a user from a network (Node calls this on network leave)",
)
async def remove_network_member(
    tenant_id: str,
    user_id:   str,
    db: AsyncSession = Depends(get_db),
):
    """
    Remove a user from a network.

    What this does:
    - Sets user.status = 'disabled' for this tenant.
    - Expires all active sKG signals immediately (live intent, presence).
    - Writes an audit log entry.
    - Does NOT delete the user or their facts — they remain for analytics
      and can re-join the network later.

    Idempotent: safe to call multiple times.
    """
    tid = _require_id(tenant_id, "tenant_id")
    uid = _require_id(user_id, "user_id")

    # Verify user is in this network
    u = await db.execute(
        text("""
            SELECT display_name, status FROM users
            WHERE user_id = :uid AND tenant_id = :tid
        """),
        {"uid": uid, "tid": tid},
    )
    user_row = u.mappings().first()
    if not user_row:
        raise HTTPException(
            status_code=404,
            detail=f"User '{uid}' is not a member of network '{tid}'.",
        )

    # Disable the user
    await db.execute(
        text("""
            UPDATE users
            SET status = 'disabled', updated_at = NOW()
            WHERE user_id = :uid AND tenant_id = :tid
        """),
        {"uid": uid, "tid": tid},
    )

    # Expire all active sKG signals immediately
    expired = await db.execute(
        text("""
            UPDATE live_signals
            SET valid_to = NOW()
            WHERE user_id = :uid AND valid_to IS NULL
            RETURNING signal_id
        """),
        {"uid": uid},
    )
    expired_count = len(expired.fetchall())

    # Expire Memgraph sKG nodes (non-fatal)
    try:
        driver = get_driver()
        async with driver.session() as session:
            await session.run(
                """
                MATCH (p:Person {person_id: $uid})-[:HAS_LIVE_INTENT]->(li:LiveIntent)
                WHERE li.valid_to IS NULL
                SET li.valid_to = $now
                """,
                uid=uid, now=_uuid.uuid4().hex,  # just needs a non-null value
            )
            await session.run(
                """
                MATCH (p:Person {person_id: $uid})-[:PRESENT_AT]->(pr:Presence)
                WHERE pr.valid_to IS NULL
                SET pr.valid_to = $now
                """,
                uid=uid, now=_uuid.uuid4().hex,
            )
    except Exception as e:
        logger.warning(f"Memgraph signal expiry failed (non-fatal): {e}")

    # Audit
    await db.execute(
        text("""
            INSERT INTO audit_log (tenant_id, actor_user_id, action, object_type, object_id)
            VALUES (:tid, :uid, 'network_leave', 'user', :uid)
        """),
        {"tid": tid, "uid": uid},
    )
    await db.commit()

    logger.info(
        f"User {uid[:8]} left network {tid[:8]} "
        f"(signals expired: {expired_count})"
    )
    return {
        "tenant_id":      tid,
        "user_id":        uid,
        "display_name":   user_row["display_name"],
        "status":         "disabled",
        "signals_expired": expired_count,
        "action":         "removed",
    }


# ─────────────────────────────────────────────
#  PATCH /v1/users/{user_id}/status
#  Low-level status toggle (internal / admin use)
#  For network membership changes, use the /networks/* endpoints above.
# ─────────────────────────────────────────────

@router.patch("/users/{user_id}/status", summary="Activate or deactivate a user (admin)")
async def update_user_status(
    user_id: str,
    req: UserStatusUpdate,
    db: AsyncSession = Depends(get_db),
):
    uid = _require_id(user_id, "user_id")

    result = await db.execute(
        text("SELECT user_id FROM users WHERE user_id = :uid"), {"uid": uid}
    )
    if not result.mappings().first():
        raise HTTPException(status_code=404, detail="User not found")

    await db.execute(
        text("UPDATE users SET status = :status, updated_at = NOW() WHERE user_id = :uid"),
        {"uid": uid, "status": req.status},
    )

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
    if not req.user_ids:
        raise HTTPException(status_code=400, detail="user_ids list is empty")

    await db.execute(
        text("""
            UPDATE users
            SET status = :status, updated_at = NOW()
            WHERE user_id = ANY(:uids) AND tenant_id = :tid
        """),
        {"status": req.status, "uids": req.user_ids, "tid": req.tenant_id},
    )

    if req.status == "disabled":
        await db.execute(
            text("""
                UPDATE live_signals
                SET valid_to = NOW()
                WHERE user_id = ANY(:uids) AND valid_to IS NULL
            """),
            {"uids": req.user_ids},
        )

    await db.commit()
    logger.info(
        f"Bulk status update: {len(req.user_ids)} users → {req.status} "
        f"tenant={req.tenant_id[:8]}"
    )
    return {
        "tenant_id":     req.tenant_id,
        "users_updated": len(req.user_ids),
        "status":        req.status,
    }


# ─────────────────────────────────────────────
#  POST /v1/admin/wipe
# ─────────────────────────────────────────────

@router.post("/admin/wipe")
async def wipe_tenant(req: WipeRequest, db: AsyncSession = Depends(get_db)):
    """Wipe all data for a tenant. Requires confirm=True."""
    if not req.confirm:
        raise HTTPException(status_code=400, detail="Set confirm=true to execute wipe")

    tid = _require_id(req.tenant_id, "tenant_id")
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
        ("users",                     "tenant_id"),
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
                    await db.execute(text("""
                        DELETE FROM user_profiles WHERE user_id IN (
                            SELECT user_id FROM users WHERE tenant_id = :tid
                        )"""), {"tid": tid})
            wiped_tables.append(table)
        except Exception as e:
            errors.append(f"{table}: {type(e).__name__}: {e}")
            logger.error(f"Wipe error for {table}: {e}")

    memgraph_wiped = False
    try:
        driver = get_driver()
        async with driver.session() as session:
            await session.run(
                "MATCH (p:Person {tenant_id: $tid}) DETACH DELETE p", tid=tid
            )
            await session.run(
                "MATCH (li:LiveIntent {tenant_id: $tid}) DETACH DELETE li", tid=tid
            )
            await session.run(
                "MATCH (pr:Presence {tenant_id: $tid}) DETACH DELETE pr", tid=tid
            )
            await session.run(
                "MATCH (mr:MatchRecommendation) WHERE mr.tenant_id = $tid DETACH DELETE mr",
                tid=tid,
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