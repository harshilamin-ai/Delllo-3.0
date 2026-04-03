"""Delllo — Tenants Router (stub, Phase 1)"""
from uuid import UUID
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.db.postgres import get_db

router = APIRouter()


@router.get("/tenants")
async def list_tenants(db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("SELECT tenant_id, name, slug, status, created_at FROM tenants ORDER BY created_at"))
    rows = result.mappings().all()
    return {"tenants": [dict(r) for r in rows]}


@router.get("/tenants/{tenant_id}")
async def get_tenant(tenant_id: UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("SELECT * FROM tenants WHERE tenant_id = :tid"),
        {"tid": str(tenant_id)}
    )
    row = result.mappings().first()
    if not row:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Tenant not found")
    return dict(row)
