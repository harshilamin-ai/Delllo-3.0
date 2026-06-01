"""
Delllo RAIN3.0 - FastAPI Application Entry Point (Phase 2)
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from app.config import settings
from app.db.postgres import init_db, close_db
from app.db.graph import init_graph, close_graph
from app.diagnostics import router as diagnostics_router
from app.routers import (
    health, tenants, profiles, signals,
    matches, ingestion, graph,
    analytics, ontology, admin,
    organisations, memberships,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"Starting Delllo API [{settings.environment}]")
    await init_db()
    await init_graph()
    print("PostgreSQL connected")
    print("Memgraph connected")
    yield
    await close_db()
    await close_graph()
    print("Delllo API shutdown complete")


app = FastAPI(
    title="Delllo RAIN3.0 API",
    description="Real-time expertise and transactional-value matching platform",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

Instrumentator().instrument(app).expose(app)

app.include_router(health.router,      prefix="",    tags=["health"])
app.include_router(admin.router,       prefix="/v1", tags=["admin"])
app.include_router(tenants.router,     prefix="/v1", tags=["tenants"])
app.include_router(profiles.router,    prefix="/v1", tags=["profiles"])
app.include_router(signals.router,     prefix="/v1", tags=["signals"])
app.include_router(matches.router,     prefix="/v1", tags=["matches"])
app.include_router(ingestion.router,   prefix="/v1", tags=["ingestion"])
app.include_router(graph.router,       prefix="/v1", tags=["graph"])
app.include_router(analytics.router,   prefix="/v1", tags=["analytics"])
app.include_router(ontology.router,       prefix="/v1", tags=["ontology"])
app.include_router(organisations.router,  prefix="/v1", tags=["organisations"])
app.include_router(memberships.router,    prefix="/v1", tags=["memberships"])
app.include_router(diagnostics_router,    prefix="/v1")