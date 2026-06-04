"""
Delllo RAIN3.0 — Ontology Overrides Router (Phase 2)

Allows tenants to customise the shared gKG scoring rules
without modifying the global ontology.

GET  /v1/ontology/{tenant_id}/overrides               List all overrides
POST /v1/ontology/{tenant_id}/overrides               Create a new override
DELETE /v1/ontology/{tenant_id}/overrides/{override_id} Remove an override
GET  /v1/ontology/{tenant_id}/effective-rules/{tx_type} Show effective rules after overrides

Override types:
  weight_boost    — increase the weight of a capability requirement
  weight_reduce   — decrease the weight of a capability requirement
  capability_add  — add a custom required capability for this tenant
  capability_block — block a capability from being used in matching
  tx_type_disable — disable a transaction type for this tenant entirely
"""

import logging
import uuid
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db.postgres import get_db
from app.db.graph import get_driver

logger = logging.getLogger(__name__)
router = APIRouter()


# ─────────────────────────────────────────────────────────────
#  Schemas
# ─────────────────────────────────────────────────────────────

class OntologyOverrideCreate(BaseModel):
    override_type: str           # weight_boost | weight_reduce | capability_add |
                                 # capability_block | tx_type_disable
    transaction_type_id: str     # e.g. "tt_technical_problem_solving"
    target_capability: Optional[str] = None   # capability canonical name
    weight_delta: Optional[float] = None      # for weight_boost / weight_reduce
    reason: Optional[str] = None


# ─────────────────────────────────────────────────────────────
#  GET /v1/ontology/{tenant_id}/overrides
# ─────────────────────────────────────────────────────────────

@router.get("/ontology/{tenant_id}/overrides")
async def list_overrides(tenant_id: UUID, db: AsyncSession = Depends(get_db)):
    """List all ontology overrides for this tenant."""
    result = await db.execute(
        text("""
            SELECT override_id, override_type, transaction_type_id,
                   target_capability, weight_delta, reason, created_at
            FROM tenant_ontology_overrides
            WHERE tenant_id = :tid
            ORDER BY created_at DESC
        """),
        {"tid": str(tenant_id)},
    )
    rows = result.mappings().all()
    return {"tenant_id": str(tenant_id), "overrides": [dict(r) for r in rows]}


# ─────────────────────────────────────────────────────────────
#  POST /v1/ontology/{tenant_id}/overrides
# ─────────────────────────────────────────────────────────────

@router.post("/ontology/{tenant_id}/overrides", status_code=201)
async def create_override(
    tenant_id: UUID,
    req: OntologyOverrideCreate,
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new ontology override for this tenant.
    The override is applied at ranking time — it does not modify the global gKG.
    """
    valid_types = {
        "weight_boost", "weight_reduce",
        "capability_add", "capability_block",
        "tx_type_disable",
    }
    if req.override_type not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"override_type must be one of: {', '.join(sorted(valid_types))}",
        )

    if req.override_type in ("weight_boost", "weight_reduce"):
        if req.weight_delta is None or req.target_capability is None:
            raise HTTPException(
                status_code=400,
                detail="weight_boost/reduce require target_capability and weight_delta",
            )

    if req.override_type in ("capability_add", "capability_block"):
        if not req.target_capability:
            raise HTTPException(
                status_code=400,
                detail="capability_add/block require target_capability",
            )

    override_id = str(uuid.uuid4())
    await db.execute(
        text("""
            INSERT INTO tenant_ontology_overrides
                (override_id, tenant_id, override_type, transaction_type_id,
                 target_capability, weight_delta, reason)
            VALUES
                (:oid, :tid, :otype, :tx_type, :cap, :wd, :reason)
        """),
        {
            "oid":    override_id,
            "tid":    str(tenant_id),
            "otype":  req.override_type,
            "tx_type": req.transaction_type_id,
            "cap":    req.target_capability,
            "wd":     req.weight_delta,
            "reason": req.reason,
        },
    )

    logger.info(
        f"Ontology override created: tenant={str(tenant_id)[:8]} "
        f"type={req.override_type} tx={req.transaction_type_id}"
    )
    return {
        "override_id":   override_id,
        "tenant_id":     str(tenant_id),
        "override_type": req.override_type,
        "status":        "created",
    }


# ─────────────────────────────────────────────────────────────
#  DELETE /v1/ontology/{tenant_id}/overrides/{override_id}
# ─────────────────────────────────────────────────────────────

@router.delete("/ontology/{tenant_id}/overrides/{override_id}")
async def delete_override(
    tenant_id: UUID,
    override_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    """Remove an ontology override. The global gKG rule is restored immediately."""
    result = await db.execute(
        text("""
            DELETE FROM tenant_ontology_overrides
            WHERE override_id = :oid AND tenant_id = :tid
            RETURNING override_id
        """),
        {"oid": str(override_id), "tid": str(tenant_id)},
    )
    row = result.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Override not found")

    return {"override_id": str(override_id), "status": "deleted"}


# ─────────────────────────────────────────────────────────────
#  GET /v1/ontology/{tenant_id}/effective-rules/{tx_type}
# ─────────────────────────────────────────────────────────────

@router.get("/ontology/{tenant_id}/effective-rules/{transaction_type_id}")
async def get_effective_rules(
    tenant_id: UUID,
    transaction_type_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Show the effective gKG rules for this transaction type after applying
    all tenant overrides. Useful for admins verifying configuration.
    """
    tid = str(tenant_id)
    driver = get_driver()

    # ── Base gKG rules from Memgraph ──────────────────────────
    try:
        async with driver.session() as session:
            r = await session.run(
                """
                MATCH (pt:ProblemType)-[:MAPS_TO]->
                      (tt:TransactionType {type_id: $type_id})
                MATCH (pt)-[req:REQUIRES]->(cap:CapabilityType)
                RETURN DISTINCT cap.canonical_name AS capability,
                               cap.name            AS capability_name,
                               req.weight          AS weight
                """,
                type_id=transaction_type_id,
            )
            base_rules = await r.data()
    except Exception as e:
        logger.error(f"gKG query failed: {e}")
        base_rules = []

    # ── Tenant overrides ──────────────────────────────────────
    overrides_result = await db.execute(
        text("""
            SELECT override_type, target_capability, weight_delta
            FROM tenant_ontology_overrides
            WHERE tenant_id = :tid
              AND transaction_type_id = :tx_type
        """),
        {"tid": tid, "tx_type": transaction_type_id},
    )
    overrides = overrides_result.mappings().all()

    # Disabled entirely?
    disabled = any(o["override_type"] == "tx_type_disable" for o in overrides)
    if disabled:
        return {
            "tenant_id":          tid,
            "transaction_type_id": transaction_type_id,
            "disabled":           True,
            "effective_rules":    [],
        }

    # Apply weight overrides
    blocked_caps = {o["target_capability"] for o in overrides
                    if o["override_type"] == "capability_block"}
    weight_deltas = {o["target_capability"]: float(o["weight_delta"] or 0.0)
                     for o in overrides
                     if o["override_type"] in ("weight_boost", "weight_reduce")}
    added_caps = [o["target_capability"] for o in overrides
                  if o["override_type"] == "capability_add"]

    effective = []
    for rule in base_rules:
        cap = rule.get("capability", "")
        if cap in blocked_caps:
            continue
        base_w = float(rule.get("weight") or 1.0)
        delta  = weight_deltas.get(cap, 0.0)
        effective.append({
            "capability":      cap,
            "capability_name": rule.get("capability_name", cap),
            "base_weight":     base_w,
            "delta":           delta,
            "effective_weight": round(max(0.0, base_w + delta), 3),
            "source":          "global_gkg",
        })

    for cap in added_caps:
        effective.append({
            "capability":      cap,
            "capability_name": cap,
            "base_weight":     0.0,
            "delta":           1.0,
            "effective_weight": 1.0,
            "source":          "tenant_override",
        })

    return {
        "tenant_id":           tid,
        "transaction_type_id": transaction_type_id,
        "disabled":            False,
        "effective_rules":     effective,
        "overrides_applied":   len(overrides),
    }