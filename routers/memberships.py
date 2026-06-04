"""
Delllo RAIN3.0 — Memberships Router

Handles user ↔ network join lifecycle: requests, approvals, suggestions, removals.

Join rule logic
───────────────
  open         → auto-approve immediately
  email_domain → auto-approve if user email domain matches rule_value
  explicit     → set status='pending'; admin must call /approve

Endpoints
─────────────────────────────────────────────────────────
POST   /v1/users/{user_id}/join                         User requests to join a network
GET    /v1/users/{user_id}/networks                     All networks the user belongs to
GET    /v1/users/{user_id}/network-suggestions          Networks the user is eligible for

POST   /v1/networks/{network_id}/approve/{user_id}      Admin approves a join request
POST   /v1/networks/{network_id}/reject/{user_id}       Admin rejects a join request
DELETE /v1/networks/{network_id}/members/{user_id}      Remove a user from a network

ID FORMAT NOTE
──────────────
Standard UUIDs and 24-char MongoDB ObjectIDs are both accepted.
"""

import logging
import re
import uuid as _uuid
from typing import Optional

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
    if _UUID_RE.match(val):
        return val
    if _MONGO_RE.match(val):
        return str(_uuid.uuid5(_MONGO_NS, val))
    raise ValueError(f"Invalid ID format: '{val}'")


# ─────────────────────────────────────────────
#  Internal helpers
# ─────────────────────────────────────────────

async def _get_user_email(uid: str, db: AsyncSession) -> Optional[str]:
    """Return the email of a user, or None if not found."""
    row = await db.execute(
        text("SELECT email FROM users WHERE user_id = :uid"),
        {"uid": uid},
    )
    r = row.mappings().first()
    return r["email"] if r else None


async def _get_network_rules(nid: str, db: AsyncSession) -> list:
    """Return all join rules for a network."""
    result = await db.execute(
        text("""
            SELECT rule_id, rule_type, rule_value
            FROM network_join_rules
            WHERE tenant_id = :nid
            ORDER BY created_at
        """),
        {"nid": nid},
    )
    return result.mappings().all()


def _email_matches_domain(email: str, domain: str) -> bool:
    """Check if an email belongs to the given domain (case-insensitive)."""
    return email.lower().endswith(f"@{domain.lower()}")


async def _resolve_join_status(email: str, rules: list) -> str:
    """
    Apply join rules in order and return the resulting membership status.

    Priority:
      1. If any 'open' rule exists → auto-approve
      2. If any 'email_domain' rule matches the user's email → auto-approve
      3. Otherwise → pending (requires admin approval)
    """
    for rule in rules:
        if rule["rule_type"] == "open":
            return "active"
        if rule["rule_type"] == "email_domain" and rule["rule_value"]:
            if _email_matches_domain(email, rule["rule_value"]):
                return "active"
    return "pending"


# ─────────────────────────────────────────────
#  Schemas
# ─────────────────────────────────────────────

class JoinRequest(BaseModel):
    network_id: str

    @field_validator("network_id", mode="before")
    @classmethod
    def validate_nid(cls, v):
        if not _is_valid(str(v)):
            raise ValueError(f"Invalid network_id: '{v}'")
        return _norm(str(v))


# ─────────────────────────────────────────────
#  POST /v1/users/{user_id}/join
# ─────────────────────────────────────────────

@router.post("/users/{user_id}/join", status_code=200, summary="Request to join a network")
async def join_network(
    user_id: str,
    req: JoinRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    User requests membership in a network.

    The join rules on the network determine the outcome:
    - open or email_domain match → status='active' (auto-approved)
    - explicit or no matching rule → status='pending' (awaits admin)

    Calling again on an existing membership returns the current state
    without duplicating the record.
    """
    uid = _norm(user_id) if _is_valid(user_id) else user_id
    nid = req.network_id

    # Verify user exists and get their email
    email = await _get_user_email(uid, db)
    if email is None:
        raise HTTPException(status_code=404, detail="User not found")

    # Verify network exists
    net_row = await db.execute(
        text("SELECT tenant_id, name FROM tenants WHERE tenant_id = :nid"),
        {"nid": nid},
    )
    network = net_row.mappings().first()
    if not network:
        raise HTTPException(status_code=404, detail="Network not found")

    # Check for existing membership
    existing = await db.execute(
        text("""
            SELECT status FROM user_tenants
            WHERE user_id = :uid AND tenant_id = :nid
        """),
        {"uid": uid, "nid": nid},
    )
    existing_row = existing.mappings().first()
    if existing_row:
        current_status = existing_row["status"]
        if current_status in ("active", "pending"):
            return {
                "user_id":    uid,
                "network_id": nid,
                "status":     current_status,
                "message":    f"Already {'a member' if current_status == 'active' else 'pending approval'}.",
            }
        # Rejected/removed → allow re-application by falling through

    # Evaluate join rules
    rules = await _get_network_rules(nid, db)
    membership_status = await _resolve_join_status(email, rules)
    joined_at = "NOW()" if membership_status == "active" else "NULL"

    await db.execute(
        text(f"""
            INSERT INTO user_tenants (user_id, tenant_id, status, role, joined_at)
            VALUES (:uid, :nid, :status, 'member', {joined_at})
            ON CONFLICT (user_id, tenant_id) DO UPDATE
                SET status    = EXCLUDED.status,
                    joined_at = CASE
                        WHEN EXCLUDED.status = 'active' THEN NOW()
                        ELSE user_tenants.joined_at
                    END
        """),
        {"uid": uid, "nid": nid, "status": membership_status},
    )
    await db.commit()

    auto_approved = membership_status == "active"
    logger.info(
        f"Join request: user={uid[:8]} network={nid[:8]} "
        f"status={membership_status} auto_approved={auto_approved}"
    )
    return {
        "user_id":      uid,
        "network_id":   nid,
        "network_name": network["name"],
        "status":       membership_status,
        "auto_approved": auto_approved,
        "message": (
            "Joined successfully — membership is active."
            if auto_approved
            else "Join request submitted — awaiting admin approval."
        ),
    }


# ─────────────────────────────────────────────
#  GET /v1/users/{user_id}/networks
# ─────────────────────────────────────────────

@router.get("/users/{user_id}/networks", summary="List all networks a user belongs to")
async def list_user_networks(user_id: str, db: AsyncSession = Depends(get_db)):
    uid = _norm(user_id) if _is_valid(user_id) else user_id

    # Verify user exists
    if not await _get_user_email(uid, db):
        raise HTTPException(status_code=404, detail="User not found")

    result = await db.execute(
        text("""
            SELECT t.tenant_id AS network_id, t.name, t.slug, t.status AS network_status,
                   ut.status AS membership_status, ut.role, ut.joined_at,
                   o.org_id, o.name AS org_name
            FROM user_tenants ut
            JOIN tenants t ON t.tenant_id = ut.tenant_id
            LEFT JOIN organisations o ON o.org_id = t.org_id
            WHERE ut.user_id = :uid
            ORDER BY ut.joined_at DESC
        """),
        {"uid": uid},
    )
    rows = result.mappings().all()
    return {
        "user_id":  uid,
        "networks": [dict(r) for r in rows],
        "count":    len(rows),
    }


# ─────────────────────────────────────────────
#  GET /v1/users/{user_id}/network-suggestions
# ─────────────────────────────────────────────

@router.get(
    "/users/{user_id}/network-suggestions",
    summary="Networks the user is eligible to join based on email domain",
)
async def network_suggestions(user_id: str, db: AsyncSession = Depends(get_db)):
    """
    Returns networks the user is eligible to join but hasn't yet.
    Eligibility is based on:
      - open networks (no restriction)
      - email_domain rules matching the user's email domain
    Networks where the user is already a member or has a pending request are excluded.
    """
    uid = _norm(user_id) if _is_valid(user_id) else user_id

    email = await _get_user_email(uid, db)
    if email is None:
        raise HTTPException(status_code=404, detail="User not found")

    # Extract user's domain for matching
    user_domain = email.split("@")[-1].lower() if "@" in email else ""

    result = await db.execute(
        text("""
            SELECT DISTINCT
                t.tenant_id AS network_id,
                t.name,
                t.slug,
                o.org_id,
                o.name AS org_name,
                njr.rule_type,
                njr.rule_value
            FROM tenants t
            JOIN network_join_rules njr ON njr.tenant_id = t.tenant_id
            LEFT JOIN organisations o ON o.org_id = t.org_id
            WHERE t.status = 'active'
              AND (
                njr.rule_type = 'open'
                OR (njr.rule_type = 'email_domain' AND njr.rule_value = :domain)
              )
              AND t.tenant_id NOT IN (
                SELECT tenant_id FROM user_tenants
                WHERE user_id = :uid AND status IN ('active', 'pending')
              )
            ORDER BY t.name
        """),
        {"uid": uid, "domain": user_domain},
    )
    rows = result.mappings().all()

    suggestions = []
    for r in rows:
        suggestions.append({
            "network_id": r["network_id"],
            "name":       r["name"],
            "slug":       r["slug"],
            "org_id":     r["org_id"],
            "org_name":   r["org_name"],
            "join_type":  r["rule_type"],   # open | email_domain (auto-join eligible)
        })

    return {
        "user_id":            uid,
        "email":              email,
        "network_suggestions": suggestions,
        "count":              len(suggestions),
    }


# ─────────────────────────────────────────────
#  POST /v1/networks/{network_id}/approve/{user_id}
# ─────────────────────────────────────────────

@router.post(
    "/networks/{network_id}/approve/{user_id}",
    summary="Admin approves a pending join request",
)
async def approve_join_request(
    network_id: str,
    user_id: str,
    db: AsyncSession = Depends(get_db),
):
    nid = _norm(network_id) if _is_valid(network_id) else network_id
    uid = _norm(user_id) if _is_valid(user_id) else user_id

    result = await db.execute(
        text("""
            UPDATE user_tenants
            SET status = 'active', joined_at = NOW()
            WHERE user_id = :uid AND tenant_id = :nid AND status = 'pending'
            RETURNING user_id
        """),
        {"uid": uid, "nid": nid},
    )
    if not result.mappings().first():
        # Check if they exist at all to give a helpful error
        existing = await db.execute(
            text("""
                SELECT status FROM user_tenants
                WHERE user_id = :uid AND tenant_id = :nid
            """),
            {"uid": uid, "nid": nid},
        )
        row = existing.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="No join request found for this user in this network")
        raise HTTPException(
            status_code=409,
            detail=f"Join request is already in status '{row['status']}' — cannot approve",
        )

    await db.commit()
    logger.info(f"Join approved: user={uid[:8]} network={nid[:8]}")
    return {
        "user_id":    uid,
        "network_id": nid,
        "status":     "active",
        "message":    "User approved and is now an active member.",
    }


# ─────────────────────────────────────────────
#  POST /v1/networks/{network_id}/reject/{user_id}
# ─────────────────────────────────────────────

@router.post(
    "/networks/{network_id}/reject/{user_id}",
    summary="Admin rejects a pending join request",
)
async def reject_join_request(
    network_id: str,
    user_id: str,
    db: AsyncSession = Depends(get_db),
):
    nid = _norm(network_id) if _is_valid(network_id) else network_id
    uid = _norm(user_id) if _is_valid(user_id) else user_id

    result = await db.execute(
        text("""
            UPDATE user_tenants
            SET status = 'rejected'
            WHERE user_id = :uid AND tenant_id = :nid AND status = 'pending'
            RETURNING user_id
        """),
        {"uid": uid, "nid": nid},
    )
    if not result.mappings().first():
        existing = await db.execute(
            text("""
                SELECT status FROM user_tenants
                WHERE user_id = :uid AND tenant_id = :nid
            """),
            {"uid": uid, "nid": nid},
        )
        row = existing.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="No join request found for this user in this network")
        raise HTTPException(
            status_code=409,
            detail=f"Join request is already in status '{row['status']}' — cannot reject",
        )

    await db.commit()
    logger.info(f"Join rejected: user={uid[:8]} network={nid[:8]}")
    return {
        "user_id":    uid,
        "network_id": nid,
        "status":     "rejected",
        "message":    "Join request rejected.",
    }


# ─────────────────────────────────────────────
#  DELETE /v1/networks/{network_id}/members/{user_id}
# ─────────────────────────────────────────────

@router.delete(
    "/networks/{network_id}/members/{user_id}",
    summary="Remove a user from a network",
)
async def remove_member(
    network_id: str,
    user_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Marks the membership as 'removed'. The user_tenants row is kept for audit history.
    Also expires any live_signals the user has in this network.
    """
    nid = _norm(network_id) if _is_valid(network_id) else network_id
    uid = _norm(user_id) if _is_valid(user_id) else user_id

    result = await db.execute(
        text("""
            UPDATE user_tenants
            SET status = 'removed'
            WHERE user_id = :uid AND tenant_id = :nid AND status = 'active'
            RETURNING user_id
        """),
        {"uid": uid, "nid": nid},
    )
    if not result.mappings().first():
        existing = await db.execute(
            text("""
                SELECT status FROM user_tenants
                WHERE user_id = :uid AND tenant_id = :nid
            """),
            {"uid": uid, "nid": nid},
        )
        row = existing.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="User is not a member of this network")
        raise HTTPException(
            status_code=409,
            detail=f"Membership is already in status '{row['status']}'",
        )

    # Expire any live signals this user had in the network
    await db.execute(
        text("""
            UPDATE live_signals
            SET valid_to = NOW()
            WHERE user_id = :uid AND tenant_id = :nid AND valid_to IS NULL
        """),
        {"uid": uid, "nid": nid},
    )

    await db.commit()
    logger.info(f"Member removed: user={uid[:8]} network={nid[:8]}")
    return {
        "user_id":    uid,
        "network_id": nid,
        "status":     "removed",
        "message":    "User removed from network. Membership record retained for audit.",
    }