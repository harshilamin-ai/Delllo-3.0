"""
Delllo — Tenants Router

GET /v1/tenants              List all tenants
GET /v1/tenants/{tenant_id}  Get a single tenant

CHANGES (org/network migration)
────────────────────────────────
• org_id now included in both list and detail responses, reflecting
  the new FK from tenants → organisations.
"""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db.postgres import get_db

router = APIRouter()


@router.get("/tenants", summary="List all tenants / networks")
async def list_tenants(db: AsyncSession = Depends(get_db)):
    """
    Returns all tenants.
    CHANGED: org_id included in response so callers can see which
    organisation each network belongs to.
    """
    result = await db.execute(
        text("""
            SELECT
                t.tenant_id,
                t.name,
                t.slug,
                t.status,
                t.org_id,
                o.name   AS org_name,
                o.slug   AS org_slug,
                t.created_at
            FROM tenants t
            LEFT JOIN organisations o ON o.org_id = t.org_id
            ORDER BY t.created_at
        """)
    )
    rows = result.mappings().all()
    return {"tenants": [dict(r) for r in rows], "count": len(rows)}

@router.get("/tenants/{tenant_id}", summary="Get a single tenant / network")
async def get_tenant(tenant_id: UUID, db: AsyncSession = Depends(get_db)):
    """
    Returns full tenant detail.
    CHANGED: org_id, org_name, and org_slug included in response.
    """
    result = await db.execute(
        text("""
            SELECT
                t.tenant_id,
                t.name,
                t.slug,
                t.status,
                t.org_id,
                o.name   AS org_name,
                o.slug   AS org_slug,
                o.domain AS org_domain,
                t.config_json,
                t.created_at
            FROM tenants t
            LEFT JOIN organisations o ON o.org_id = t.org_id
            WHERE t.tenant_id = :tid
        """),
        {"tid": str(tenant_id)},
    )
    row = result.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return dict(row)