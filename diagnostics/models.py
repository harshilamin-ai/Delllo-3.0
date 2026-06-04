"""
Delllo RAIN3.0 — Diagnostic Models

Pydantic models for all diagnostic check results and reports.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class DiagnosticStatus(str, Enum):
    OK      = "ok"
    WARN    = "warn"
    ERROR   = "error"
    SKIP    = "skip"


# ─────────────────────────────────────────────────────────────
#  Single check result
# ─────────────────────────────────────────────────────────────

class DiagnosticCheckResult(BaseModel):
    service:    str
    status:     DiagnosticStatus
    detail:     str
    latency_ms: Optional[float]  = None
    metadata:   dict[str, Any]   = Field(default_factory=dict)
    timestamp:  datetime         = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


# ─────────────────────────────────────────────────────────────
#  Pipeline stage
# ─────────────────────────────────────────────────────────────

class PipelineStageStatus(BaseModel):
    stage:       str
    status:      DiagnosticStatus
    detail:      str
    duration_ms: Optional[float] = None
    metadata:    dict[str, Any]  = Field(default_factory=dict)


# ─────────────────────────────────────────────────────────────
#  Matchmaking-specific check results
# ─────────────────────────────────────────────────────────────

class EmbeddingCoverageResult(BaseModel):
    """Percentage of document chunks that have non-null embeddings."""
    total_chunks:       int
    embedded_chunks:    int
    coverage_pct:       float             # 0–100
    users_with_zero_embeddings: list[str] # user_ids with docs but no embeddings
    status:             DiagnosticStatus
    detail:             str


class GKGSeedingResult(BaseModel):
    """Whether Memgraph has transaction type → capability chains seeded."""
    transaction_types_checked: list[str]
    seeded_types:     list[str]           # have at least 1 path
    empty_types:      list[str]           # no ProblemType→TransactionType path
    total_nodes:      int
    total_edges:      int
    status:           DiagnosticStatus
    detail:           str


class FactCoverageResult(BaseModel):
    """Per-tenant fact extraction quality."""
    total_users:          int
    users_with_facts:     int
    cold_start_users:     int             # zero facts
    users_only_engine_visible: int        # all facts are match_engine_only
    avg_facts_per_user:   float
    low_confidence_users: int             # avg confidence < 0.5
    status:               DiagnosticStatus
    detail:               str


class RetrievalSanityResult(BaseModel):
    """End-to-end retrieval test: does a known user get candidates back?"""
    test_user_id:       str
    transaction_type:   str
    semantic_hits:      int
    graph_hits:         int
    merged_candidates:  int
    after_hard_filter:  int
    used_fallback:      bool
    status:             DiagnosticStatus
    detail:             str


class ScoreSanityResult(BaseModel):
    """Are match scores meaningfully distributed, or stuck at floor/ceiling?"""
    sample_size:        int
    min_score:          float
    max_score:          float
    avg_score:          float
    pct_below_0_2:      float             # % of matches scoring below 0.2
    pct_above_0_7:      float             # % of matches scoring above 0.7
    all_zero_features:  list[str]         # feature columns that are 0 for every match
    status:             DiagnosticStatus
    detail:             str


class SelfTestResult(BaseModel):
    """Full end-to-end self-test: ingest → extract → signal → match → assert."""
    tenant_id:          str
    test_user_id:       str
    stages:             list[PipelineStageStatus]
    matches_generated:  int
    top_score:          Optional[float]
    passed:             bool
    status:             DiagnosticStatus
    detail:             str
    duration_ms:        float


# ─────────────────────────────────────────────────────────────
#  Aggregated report
# ─────────────────────────────────────────────────────────────

class MatchmakingDiagnosticReport(BaseModel):
    tenant_id:          str
    timestamp:          datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    overall_status:     DiagnosticStatus

    # Infrastructure (from existing health.py checks)
    infra_checks:       list[DiagnosticCheckResult] = Field(default_factory=list)

    # Matchmaking-specific
    embedding_coverage: Optional[EmbeddingCoverageResult]  = None
    gkg_seeding:        Optional[GKGSeedingResult]         = None
    fact_coverage:      Optional[FactCoverageResult]       = None
    retrieval_sanity:   Optional[RetrievalSanityResult]    = None
    score_sanity:       Optional[ScoreSanityResult]        = None

    # Summary counters
    checks_ok:          int = 0
    checks_warn:        int = 0
    checks_error:       int = 0

    # Human-readable action list
    actions_required:   list[str] = Field(default_factory=list)
    warnings:           list[str] = Field(default_factory=list)