"""
Delllo RAIN3.0 — Organisations Router

Organisations are the top-level entity that own one or more Networks (tenants).
Networks are always scoped under an org.

Endpoints
─────────────────────────────────────────────────────────
POST   /v1/organisations                              Create an organisation
GET    /v1/organisations                              List all organisations
GET    /v1/organisations/{org_id}                     Get org detail
PATCH  /v1/organisations/{org_id}                     Update org name / domain / status

POST   /v1/organisations/{org_id}/networks            Create a network under this org
GET    /v1/organisations/{org_id}/networks            List all networks for this org

POST   /v1/networks/{network_id}/rules                Add a join rule
GET    /v1/networks/{network_id}/rules                List join rules for a network
DELETE /v1/networks/{network_id}/rules/{rule_id}      Remove a join rule

GET    /v1/networks/{network_id}/members              List active members of a network

ID FORMAT NOTE
──────────────
Standard UUIDs and 24-char MongoDB ObjectIDs are both accepted.
mongo_to_uuid() converts ObjectIDs to deterministic UUID-v5 for UUID columns.
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

logger = logging.getLogger(__name__)
router = APIRouter()

# ─────────────────────────────────────────────
#  ID helpers (mirrors admin.py)
# ─────────────────────────────────────────────

_UUID_RE  = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I
)
_MONGO_RE = re.compile(r'^[0-9a-f]{24}$', re.I)
_MONGO_NS = _uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


def _is_valid(val: str) -> bool:
    return bool(_UUID_RE.match(val) or _MONGO_RE.match(val))


def _norm(val: str) -> str:
    """UUID passes through. MongoDB ObjectID → deterministic UUID-v5."""
    if _UUID_RE.match(val):
        return val
    if _MONGO_RE.match(val):
        return str(_uuid.uuid5(_MONGO_NS, val))
    raise ValueError(f"Invalid ID format: '{val}'")


def _norm_opt(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    if not _is_valid(val):
        raise ValueError(f"Invalid ID format: '{val}'")
    return _norm(val)


# ─────────────────────────────────────────────
#  Schemas
# ─────────────────────────────────────────────

class OrgCreate(BaseModel):
    name:   str
    slug:   str
    domain: Optional[str] = None         # e.g. "haptec.com"
    config: Optional[dict] = None


class OrgUpdate(BaseModel):
    name:   Optional[str] = None
    domain: Optional[str] = None
    status: Optional[str] = None         # active | suspended

    @field_validator("status")
    @classmethod
    def check_status(cls, v):
        if v is not None and v not in ("active", "suspended"):
            raise ValueError("status must be 'active' or 'suspended'")
        return v


class NetworkCreate(BaseModel):
    network_id: Optional[str] = None     # auto-generated if omitted
    name:       str
    slug:       str
    config:     Optional[dict] = None

    @field_validator("network_id", mode="before")
    @classmethod
    def validate_nid(cls, v):
        if not v:
            return None
        if not _is_valid(str(v)):
            raise ValueError(f"Invalid network_id: '{v}'")
        return _norm(str(v))


class JoinRuleCreate(BaseModel):
    rule_type:  str                      # email_domain | open | explicit
    rule_value: Optional[str] = None     # domain value for email_domain; null otherwise
    created_by: Optional[str] = None     # user_id of the admin adding the rule

    @field_validator("rule_type")
    @classmethod
    def check_rule_type(cls, v):
        if v not in ("email_domain", "open", "explicit"):
            raise ValueError("rule_type must be 'email_domain', 'open', or 'explicit'")
        return v

    @field_validator("created_by", mode="before")
    @classmethod
    def validate_creator(cls, v):
        return _norm_opt(v)


# ─────────────────────────────────────────────
#  POST /v1/organisations
# ─────────────────────────────────────────────

@router.post("/organisations", status_code=201, summary="Create an organisation")
async def create_organisation(req: OrgCreate, db: AsyncSession = Depends(get_db)):
    """
    Creates a new organisation. An org owns one or more networks.
    Returns the org_id for use in subsequent network creation calls.
    """
    slug = req.slug.lower().strip().replace(" ", "-")

    # Slug uniqueness check
    existing = await db.execute(
        text("SELECT org_id FROM organisations WHERE slug = :slug"),
        {"slug": slug},
    )
    if existing.mappings().first():
        raise HTTPException(
            status_code=409,
            detail=f"An organisation with slug '{slug}' already exists.",
        )

    org_id = str(_uuid.uuid4())
    await db.execute(
        text("""
            INSERT INTO organisations (org_id, name, slug, domain, status, config_json)
            VALUES (:org_id, :name, :slug, :domain, 'active', CAST(:config AS JSONB))
        """),
        {
            "org_id": org_id,
            "name":   req.name,
            "slug":   slug,
            "domain": req.domain,
            "config": json.dumps(req.config or {}),
        },
    )
    await db.commit()

    logger.info(f"Organisation created: {org_id[:8]} slug={slug}")
    return {
        "org_id": org_id,
        "name":   req.name,
        "slug":   slug,
        "domain": req.domain,
        "status": "active",
    }


# ─────────────────────────────────────────────
#  GET /v1/organisations
# ─────────────────────────────────────────────

@router.get("/organisations", summary="List all organisations")
async def list_organisations(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("""
            SELECT o.org_id, o.name, o.slug, o.domain, o.status, o.created_at,
                   COUNT(t.tenant_id) AS network_count
            FROM organisations o
            LEFT JOIN tenants t ON t.org_id = o.org_id
            GROUP BY o.org_id
            ORDER BY o.created_at DESC
        """)
    )
    rows = result.mappings().all()
    return {"organisations": [dict(r) for r in rows], "count": len(rows)}


# ─────────────────────────────────────────────
#  GET /v1/organisations/{org_id}
# ─────────────────────────────────────────────

@router.get("/organisations/{org_id}", summary="Get organisation detail")
async def get_organisation(org_id: str, db: AsyncSession = Depends(get_db)):
    oid = _norm(org_id) if _is_valid(org_id) else org_id

    result = await db.execute(
        text("""
            SELECT o.org_id, o.name, o.slug, o.domain, o.status,
                   o.config_json, o.created_at,
                   COUNT(t.tenant_id) AS network_count
            FROM organisations o
            LEFT JOIN tenants t ON t.org_id = o.org_id
            WHERE o.org_id = :oid
            GROUP BY o.org_id
        """),
        {"oid": oid},
    )
    row = result.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Organisation not found")
    return dict(row)


# ─────────────────────────────────────────────
#  PATCH /v1/organisations/{org_id}
# ─────────────────────────────────────────────

@router.patch("/organisations/{org_id}", summary="Update organisation")
async def update_organisation(
    org_id: str,
    req: OrgUpdate,
    db: AsyncSession = Depends(get_db),
):
    oid = _norm(org_id) if _is_valid(org_id) else org_id

    # Verify org exists
    existing = await db.execute(
        text("SELECT org_id, name, domain, status FROM organisations WHERE org_id = :oid"),
        {"oid": oid},
    )
    row = existing.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Organisation not found")

    # Build partial update — only touch supplied fields
    updates: list[str] = []
    params: dict = {"oid": oid}

    if req.name is not None:
        updates.append("name = :name")
        params["name"] = req.name
    if req.domain is not None:
        updates.append("domain = :domain")
        params["domain"] = req.domain
    if req.status is not None:
        updates.append("status = :status")
        params["status"] = req.status

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    updates.append("updated_at = NOW()")
    await db.execute(
        text(f"UPDATE organisations SET {', '.join(updates)} WHERE org_id = :oid"),
        params,
    )
    await db.commit()

    logger.info(f"Organisation {oid[:8]} updated: {list(params.keys())}")
    return {
        "org_id": oid,
        "updated": {k: v for k, v in params.items() if k != "oid"},
    }


# ─────────────────────────────────────────────
#  POST /v1/organisations/{org_id}/networks
# ─────────────────────────────────────────────

@router.post(
    "/organisations/{org_id}/networks",
    status_code=201,
    summary="Create a network under an organisation",
)
async def create_network(
    org_id: str,
    req: NetworkCreate,
    db: AsyncSession = Depends(get_db),
):
    """
    Creates a tenant/network scoped under an org.
    The tenant is created in the existing `tenants` table with org_id set.
    """
    oid = _norm(org_id) if _is_valid(org_id) else org_id

    # Verify org exists
    org_row = await db.execute(
        text("SELECT org_id FROM organisations WHERE org_id = :oid"),
        {"oid": oid},
    )
    if not org_row.mappings().first():
        raise HTTPException(status_code=404, detail="Organisation not found")

    slug = req.slug.lower().strip().replace(" ", "-")

    # Slug uniqueness within tenants
    existing = await db.execute(
        text("SELECT tenant_id FROM tenants WHERE slug = :slug"),
        {"slug": slug},
    )
    if existing.mappings().first():
        raise HTTPException(
            status_code=409,
            detail=f"A network with slug '{slug}' already exists.",
        )

    nid = req.network_id or str(_uuid.uuid4())
    await db.execute(
        text("""
            INSERT INTO tenants (tenant_id, name, slug, status, config_json, org_id)
            VALUES (:nid, :name, :slug, 'active', CAST(:config AS JSONB), :oid)
        """),
        {
            "nid":    nid,
            "name":   req.name,
            "slug":   slug,
            "config": json.dumps(req.config or {}),
            "oid":    oid,
        },
    )
    await db.commit()

    logger.info(f"Network created: {nid[:8]} slug={slug} org={oid[:8]}")
    return {
        "network_id": nid,
        "org_id":     oid,
        "name":       req.name,
        "slug":       slug,
        "status":     "active",
    }


# ─────────────────────────────────────────────
#  GET /v1/organisations/{org_id}/networks
# ─────────────────────────────────────────────

@router.get("/organisations/{org_id}/networks", summary="List networks for an organisation")
async def list_networks(org_id: str, db: AsyncSession = Depends(get_db)):
    oid = _norm(org_id) if _is_valid(org_id) else org_id

    org_row = await db.execute(
        text("SELECT org_id FROM organisations WHERE org_id = :oid"),
        {"oid": oid},
    )
    if not org_row.mappings().first():
        raise HTTPException(status_code=404, detail="Organisation not found")

    result = await db.execute(
        text("""
            SELECT t.tenant_id AS network_id, t.name, t.slug, t.status, t.created_at,
                   COUNT(ut.user_id) FILTER (WHERE ut.status = 'active') AS active_member_count
            FROM tenants t
            LEFT JOIN user_tenants ut ON ut.tenant_id = t.tenant_id
            WHERE t.org_id = :oid
            GROUP BY t.tenant_id
            ORDER BY t.created_at DESC
        """),
        {"oid": oid},
    )
    rows = result.mappings().all()
    return {"org_id": oid, "networks": [dict(r) for r in rows], "count": len(rows)}


# ─────────────────────────────────────────────
#  POST /v1/networks/{network_id}/rules
# ─────────────────────────────────────────────

@router.post(
    "/networks/{network_id}/rules",
    status_code=201,
    summary="Add a join rule to a network",
)
async def add_join_rule(
    network_id: str,
    req: JoinRuleCreate,
    db: AsyncSession = Depends(get_db),
):
    """
    Adds a join rule to a network.
    - email_domain: auto-approve users whose email matches rule_value (e.g. "haptec.com")
    - open: anyone can join without approval
    - explicit: admin must manually approve every request

    Only one 'open' rule is allowed per network (enforced by DB unique index).
    """
    nid = _norm(network_id) if _is_valid(network_id) else network_id

    # Verify network exists
    net_row = await db.execute(
        text("SELECT tenant_id FROM tenants WHERE tenant_id = :nid"),
        {"nid": nid},
    )
    if not net_row.mappings().first():
        raise HTTPException(status_code=404, detail="Network not found")

    # email_domain requires a rule_value
    if req.rule_type == "email_domain" and not req.rule_value:
        raise HTTPException(
            status_code=400,
            detail="rule_value (domain) is required for rule_type 'email_domain'",
        )

    # Duplicate rule guard for email_domain
    if req.rule_type == "email_domain" and req.rule_value:
        dup = await db.execute(
            text("""
                SELECT rule_id FROM network_join_rules
                WHERE tenant_id = :nid AND rule_type = 'email_domain'
                  AND rule_value = :val
            """),
            {"nid": nid, "val": req.rule_value},
        )
        if dup.mappings().first():
            raise HTTPException(
                status_code=409,
                detail=f"An email_domain rule for '{req.rule_value}' already exists on this network.",
            )

    rule_id = str(_uuid.uuid4())
    await db.execute(
        text("""
            INSERT INTO network_join_rules (rule_id, tenant_id, rule_type, rule_value, created_by)
            VALUES (:rule_id, :nid, :rule_type, :rule_value, :created_by)
        """),
        {
            "rule_id":    rule_id,
            "nid":        nid,
            "rule_type":  req.rule_type,
            "rule_value": req.rule_value,
            "created_by": req.created_by,
        },
    )
    await db.commit()

    logger.info(f"Join rule added: {rule_id[:8]} type={req.rule_type} network={nid[:8]}")
    return {
        "rule_id":    rule_id,
        "network_id": nid,
        "rule_type":  req.rule_type,
        "rule_value": req.rule_value,
    }


# ─────────────────────────────────────────────
#  GET /v1/networks/{network_id}/rules
# ─────────────────────────────────────────────

@router.get("/networks/{network_id}/rules", summary="List join rules for a network")
async def list_join_rules(network_id: str, db: AsyncSession = Depends(get_db)):
    nid = _norm(network_id) if _is_valid(network_id) else network_id

    net_row = await db.execute(
        text("SELECT tenant_id FROM tenants WHERE tenant_id = :nid"),
        {"nid": nid},
    )
    if not net_row.mappings().first():
        raise HTTPException(status_code=404, detail="Network not found")

    result = await db.execute(
        text("""
            SELECT rule_id, rule_type, rule_value, created_by, created_at
            FROM network_join_rules
            WHERE tenant_id = :nid
            ORDER BY created_at
        """),
        {"nid": nid},
    )
    rows = result.mappings().all()
    return {"network_id": nid, "rules": [dict(r) for r in rows], "count": len(rows)}


# ─────────────────────────────────────────────
#  DELETE /v1/networks/{network_id}/rules/{rule_id}
# ─────────────────────────────────────────────

@router.delete(
    "/networks/{network_id}/rules/{rule_id}",
    summary="Remove a join rule from a network",
)
async def delete_join_rule(
    network_id: str,
    rule_id: str,
    db: AsyncSession = Depends(get_db),
):
    nid = _norm(network_id) if _is_valid(network_id) else network_id
    rid = _norm(rule_id) if _is_valid(rule_id) else rule_id

    result = await db.execute(
        text("""
            DELETE FROM network_join_rules
            WHERE rule_id = :rid AND tenant_id = :nid
            RETURNING rule_id
        """),
        {"rid": rid, "nid": nid},
    )
    deleted = result.mappings().first()
    if not deleted:
        raise HTTPException(
            status_code=404,
            detail="Rule not found on this network",
        )

    await db.commit()
    logger.info(f"Join rule deleted: {rid[:8]} network={nid[:8]}")
    return {"rule_id": rid, "network_id": nid, "deleted": True}


# ─────────────────────────────────────────────
#  GET /v1/networks/{network_id}/members
# ─────────────────────────────────────────────

@router.get("/networks/{network_id}/members", summary="List active members of a network")
async def list_network_members(
    network_id: str,
    db: AsyncSession = Depends(get_db),
):
    nid = _norm(network_id) if _is_valid(network_id) else network_id

    net_row = await db.execute(
        text("SELECT tenant_id FROM tenants WHERE tenant_id = :nid"),
        {"nid": nid},
    )
    if not net_row.mappings().first():
        raise HTTPException(status_code=404, detail="Network not found")

    result = await db.execute(
        text("""
            SELECT u.user_id, u.display_name, u.email, u.status AS user_status,
                   ut.role, ut.joined_at,
                   p.headline, p.home_location
            FROM user_tenants ut
            JOIN users u ON u.user_id = ut.user_id
            LEFT JOIN user_profiles p ON p.user_id = u.user_id
            WHERE ut.tenant_id = :nid AND ut.status = 'active'
            ORDER BY ut.joined_at DESC
        """),
        {"nid": nid},
    )
    rows = result.mappings().all()
    return {
        "network_id": nid,
        "members":    [dict(r) for r in rows],
        "count":      len(rows),
    }
