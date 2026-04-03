"""Delllo — Profiles Router (stub, Phase 1)"""
from uuid import UUID
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.db.postgres import get_db

router = APIRouter()


@router.get("/profiles/{user_id}")
async def get_profile(user_id: UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("""
            SELECT u.user_id, u.display_name, u.email, u.role,
                   p.headline, p.summary, p.home_location, p.default_visibility
            FROM users u
            LEFT JOIN user_profiles p ON p.user_id = u.user_id
            WHERE u.user_id = :uid
        """),
        {"uid": str(user_id)},
    )
    row = result.mappings().first()
    if not row:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="User not found")
    return dict(row)


@router.get("/profiles/{user_id}/facts")
async def get_profile_facts(user_id: UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("""
            SELECT fact_id, fact_type, canonical_value, raw_value,
                   confidence, visibility, validated_by_user, freshness_date
            FROM extracted_facts
            WHERE user_id = :uid
            ORDER BY confidence DESC
        """),
        {"uid": str(user_id)},
    )
    rows = result.mappings().all()
    return {"user_id": str(user_id), "facts": [dict(r) for r in rows]}
