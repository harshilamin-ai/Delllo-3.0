"""
Delllo — Health Check Endpoints
GET /health         → overall status
GET /health/live    → liveness probe (k8s)
GET /health/ready   → readiness probe (k8s)
GET /health/stack   → detailed per-service status
"""

import httpx
from fastapi import APIRouter
from sqlalchemy import text

from app.db.postgres import AsyncSessionLocal
from app.db.graph import get_driver
from app.config import settings

router = APIRouter()


async def _check_postgres() -> dict:
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(text("SELECT version()"))
            version = result.scalar()
        return {"status": "ok", "detail": version.split(",")[0]}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


async def _check_memgraph() -> dict:
    try:
        driver = get_driver()
        async with driver.session() as s:
            result = await s.run("RETURN 1 AS ping")
            await result.single()
        return {"status": "ok", "detail": "Bolt connection healthy"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


async def _check_ollama() -> dict:
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(f"{settings.ollama_base_url}/api/tags")
            if resp.status_code == 200:
                models = [m["name"] for m in resp.json().get("models", [])]
                target_ok = settings.ollama_model in models
                return {
                    "status": "ok" if target_ok else "warn",
                    "detail": f"Model '{settings.ollama_model}' {'found' if target_ok else 'NOT FOUND — run: ollama pull ' + settings.ollama_model}",
                    "available_models": models,
                }
            return {"status": "warn", "detail": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


async def _check_minio() -> dict:
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            protocol = "https" if settings.minio_secure else "http"
            resp = await client.get(f"{protocol}://{settings.minio_endpoint}/minio/health/live")
            return {"status": "ok" if resp.status_code == 200 else "warn", "detail": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@router.get("/health")
async def health():
    """Quick overall health — returns 200 if app is running."""
    return {
        "status": "ok",
        "service": "delllo-api",
        "version": "0.1.0",
        "environment": settings.environment,
    }


@router.get("/health/live")
async def liveness():
    """Kubernetes liveness probe — just confirm app process is alive."""
    return {"alive": True}


@router.get("/health/ready")
async def readiness():
    """Kubernetes readiness probe — confirm all critical dependencies are up."""
    pg   = await _check_postgres()
    mg   = await _check_memgraph()

    all_ok = pg["status"] == "ok" and mg["status"] == "ok"

    return {
        "ready": all_ok,
        "postgres":  pg,
        "memgraph":  mg,
    }


@router.get("/health/stack")
async def stack_health():
    """Full stack check — includes Ollama and MinIO."""
    pg      = await _check_postgres()
    mg      = await _check_memgraph()
    ollama  = await _check_ollama()
    minio   = await _check_minio()

    services = {
        "postgres": pg,
        "memgraph": mg,
        "ollama":   ollama,
        "minio":    minio,
    }

    overall = "ok" if all(v["status"] == "ok" for v in services.values()) else \
              "degraded" if any(v["status"] == "ok" for v in services.values()) else \
              "down"

    return {
        "overall": overall,
        "services": services,
        "environment": settings.environment,
        "ollama_model_target": settings.ollama_model,
    }
