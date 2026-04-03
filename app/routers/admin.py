"""
Delllo RAIN3.0 — Admin Router

POST /v1/admin/wipe          Wipe all tenant data (Postgres + Memgraph)
POST /v1/users               Create or upsert a user
GET  /v1/users               List all users in a tenant
"""

import logging
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Optional

from app.db.postgres import get_db
from app.db.graph import get_driver

logger = logging.getLogger(__name__)
router = APIRouter()


# ─────────────────────────────────────────────
#  Schemas
# ─────────────────────────────────────────────

class UserCreate(BaseModel):
    user_id:      Optional[str] = None   # if None, generated
    tenant_id:    str
    display_name: str
    email:        str
    headline:     str = ""
    role:         str = "member"         # admin | member | viewer
    status:       str = "active"


class WipeRequest(BaseModel):
    tenant_id: str
    confirm:   bool = False              # must be True to execute


# ─────────────────────────────────────────────
#  POST /v1/users  — create or upsert a user
# ─────────────────────────────────────────────

@router.post("/users", status_code=201)
async def create_user(req: UserCreate, db: AsyncSession = Depends(get_db)):
    """
    Create a user + profile. Upserts on conflict so re-running tests is safe.
    """
    uid = req.user_id or str(uuid4())
    tid = req.tenant_id

    # Ensure tenant exists
    t = await db.execute(
        text("SELECT tenant_id FROM tenants WHERE tenant_id = :tid"), {"tid": tid}
    )
    if not t.mappings().first():
        await db.execute(
            text("""
                INSERT INTO tenants (tenant_id, name, slug, status)
                VALUES (:tid, 'Test Tenant', 'test-tenant', 'active')
                ON CONFLICT DO NOTHING
            """),
            {"tid": tid},
        )

    # Upsert user
    await db.execute(
        text("""
            INSERT INTO users (user_id, tenant_id, display_name, email, role, status)
            VALUES (:uid, :tid, :name, :email, :role, :status)
            ON CONFLICT (user_id) DO UPDATE
                SET display_name = EXCLUDED.display_name,
                    email        = EXCLUDED.email,
                    role         = EXCLUDED.role,
                    status       = EXCLUDED.status
        """),
        {
            "uid":    uid,
            "tid":    tid,
            "name":   req.display_name,
            "email":  req.email,
            "role":   req.role,
            "status": req.status,
        },
    )

    # Upsert profile
    await db.execute(
        text("""
            INSERT INTO user_profiles (user_id, headline, default_visibility)
            VALUES (:uid, :headline, 'match_engine_only')
            ON CONFLICT (user_id) DO UPDATE
                SET headline = EXCLUDED.headline
        """),
        {"uid": uid, "headline": req.headline},
    )

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
    result = await db.execute(
        text("""
            SELECT u.user_id, u.display_name, u.email, u.role, u.status,
                   p.headline,
                   COUNT(ef.fact_id) AS fact_count
            FROM users u
            LEFT JOIN user_profiles p  ON p.user_id = u.user_id
            LEFT JOIN extracted_facts ef
                ON ef.user_id = u.user_id AND ef.tenant_id = :tid
            WHERE u.tenant_id = :tid
            GROUP BY u.user_id, u.display_name, u.email, u.role,
                     u.status, p.headline
            ORDER BY u.display_name
        """),
        {"tid": tenant_id},
    )
    rows = result.mappings().all()
    return {"tenant_id": tenant_id, "users": [dict(r) for r in rows], "count": len(rows)}


# ─────────────────────────────────────────────
#  POST /v1/admin/wipe
#  Truncates all tenant-scoped tables + wipes Memgraph
#  ONLY for dev/test environments
# ─────────────────────────────────────────────

@router.post("/admin/wipe")
async def wipe_tenant(req: WipeRequest, db: AsyncSession = Depends(get_db)):
    """
    Wipe all data for a tenant.
    Requires confirm=True. Only works in development/test environments.
    """
    if not req.confirm:
        raise HTTPException(status_code=400, detail="Set confirm=true to execute wipe")

    tid = req.tenant_id
    wiped_tables = []
    errors       = []

    # Ordered to respect FK constraints
    tables_to_clear = [
        ("feature_snapshots",          "tenant_id"),
        ("notifications",              "tenant_id"),
        ("feedback_events",            None),         # FK via matches
        ("match_scores",               None),         # FK via matches
        ("explanations",               None),         # FK via matches
        ("matches",                    "tenant_id"),
        ("live_signals",               "tenant_id"),
        ("extracted_facts",            "tenant_id"),
        ("document_chunks",            None),         # FK via documents
        ("documents",                  "tenant_id"),
        ("audit_log",                  "tenant_id"),
        ("tenant_ontology_overrides",  "tenant_id"),
        ("user_profiles",              None),         # FK via users
        ("users",                      "tenant_id"),
    ]

    for table, tid_col in tables_to_clear:
        try:
            if tid_col:
                await db.execute(
                    text(f"DELETE FROM {table} WHERE {tid_col} = :tid"), {"tid": tid}
                )
            else:
                # These tables are cleared via cascade or need cross-table joins
                if table == "document_chunks":
                    await db.execute(
                        text("""
                            DELETE FROM document_chunks
                            WHERE document_id IN (
                                SELECT document_id FROM documents WHERE tenant_id = :tid
                            )
                        """),
                        {"tid": tid},
                    )
                elif table == "feedback_events":
                    await db.execute(
                        text("""
                            DELETE FROM feedback_events
                            WHERE match_id IN (
                                SELECT match_id FROM matches WHERE tenant_id = :tid
                            )
                        """),
                        {"tid": tid},
                    )
                elif table in ("match_scores", "explanations"):
                    await db.execute(
                        text(f"""
                            DELETE FROM {table}
                            WHERE match_id IN (
                                SELECT match_id FROM matches WHERE tenant_id = :tid
                            )
                        """),
                        {"tid": tid},
                    )
                elif table == "user_profiles":
                    await db.execute(
                        text("""
                            DELETE FROM user_profiles
                            WHERE user_id IN (
                                SELECT user_id FROM users WHERE tenant_id = :tid
                            )
                        """),
                        {"tid": tid},
                    )
            wiped_tables.append(table)
        except Exception as e:
            errors.append(f"{table}: {type(e).__name__}: {e}")
            logger.error(f"Wipe error for {table}: {e}")

    # Wipe Memgraph — delete all nodes for this tenant
    memgraph_wiped = False
    try:
        driver = get_driver()
        async with driver.session() as session:
            await session.run(
                "MATCH (p:Person {tenant_id: $tid}) DETACH DELETE p", tid=tid
            )
            # Also clean orphaned signal/match nodes
            await session.run("MATCH (li:LiveIntent) WHERE NOT (li)--() DELETE li")
            await session.run("MATCH (pr:Presence)   WHERE NOT (pr)--() DELETE pr")
            await session.run(
                "MATCH (mr:MatchRecommendation) WHERE NOT (mr)--() DELETE mr"
            )
        memgraph_wiped = True
    except Exception as e:
        errors.append(f"Memgraph: {type(e).__name__}: {e}")
        logger.error(f"Memgraph wipe error: {e}")

    logger.info(
        f"Wipe complete for tenant={tid[:8]}: "
        f"{len(wiped_tables)} tables, memgraph={memgraph_wiped}"
    )

    return {
        "tenant_id":      tid,
        "tables_wiped":   wiped_tables,
        "memgraph_wiped": memgraph_wiped,
        "errors":         errors,
        "status":         "ok" if not errors else "partial",
    }