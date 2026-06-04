"""
Delllo RAIN3.0 — Ingestion & Extraction Pydantic Schemas
"""

from __future__ import annotations

from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, field_validator


# ─────────────────────────────────────────────────────────────
#  Core fact sub-models
# ─────────────────────────────────────────────────────────────

class SkillFact(BaseModel):
    name: str
    canonical_name: str = ""
    confidence: float = 0.7
    evidence_ref: str = ""
    visibility: str = "match_engine_only"
    class Config:
        extra = "ignore"


class DomainFact(BaseModel):
    name: str
    canonical_name: str = ""
    confidence: float = 0.7
    evidence_ref: str = ""
    visibility: str = "match_engine_only"
    class Config:
        extra = "ignore"


class ObjectiveFact(BaseModel):
    text: str
    urgency: str = "medium"
    visibility: str = "match_engine_only"
    class Config:
        extra = "ignore"


class OfferFact(BaseModel):
    text: str
    confidence: float = 0.75
    visibility: str = "match_engine_only"
    class Config:
        extra = "ignore"


class AchievementFact(BaseModel):
    text: str
    confidence: float = 0.7
    evidence_ref: str = ""
    visibility: str = "match_engine_only"
    class Config:
        extra = "ignore"


# ─────────────────────────────────────────────────────────────
#  Extended fact sub-models — fields must match graph_writer.py
# ─────────────────────────────────────────────────────────────

class TopicFact(BaseModel):
    """Fields consumed by graph_writer.upsert_topic(): name, canonical_name"""
    name: str
    canonical_name: str = ""
    confidence: float = 0.7
    visibility: str = "match_engine_only"
    class Config:
        extra = "ignore"


class NeedFact(BaseModel):
    """Fields consumed by graph_writer.upsert_need(): text, urgency"""
    text: str
    urgency: str = "medium"
    confidence: float = 0.7
    visibility: str = "match_engine_only"
    class Config:
        extra = "ignore"


class AssetFact(BaseModel):
    """Fields consumed by graph_writer.upsert_asset(): name, asset_type, description, url_or_ref"""
    name: str
    asset_type: str = "publication"   # publication | tool | model | dataset | other
    description: Optional[str] = None
    url_or_ref: Optional[str] = None
    confidence: float = 0.75
    visibility: str = "match_engine_only"
    class Config:
        extra = "ignore"


class ProjectFact(BaseModel):
    """Fields consumed by graph_writer.upsert_project(): name, description, role, organisation"""
    name: str
    description: Optional[str] = None
    role: Optional[str] = None
    organisation: Optional[str] = None
    confidence: float = 0.7
    visibility: str = "match_engine_only"
    class Config:
        extra = "ignore"


class LocationFact(BaseModel):
    """Fields consumed by graph_writer.upsert_location(): site, floor, city"""
    site: str
    floor: Optional[str] = None
    city: Optional[str] = None
    confidence: float = 0.85
    visibility: str = "match_engine_only"

    @field_validator("floor", "city", mode="before")
    @classmethod
    def coerce_to_str(cls, v):
        if v is None:
            return None
        return str(v)

    class Config:
        extra = "ignore"


class ConstraintFact(BaseModel):
    """Fields consumed by graph_writer.upsert_constraint(): text, constraint_type"""
    text: str
    constraint_type: str = "availability"  # availability | time | scope | other
    confidence: float = 0.8
    visibility: str = "match_engine_only"
    class Config:
        extra = "ignore"


# ─────────────────────────────────────────────────────────────
#  ExtractionResult — full parsed LLM output
# ─────────────────────────────────────────────────────────────

class ExtractionResult(BaseModel):
    person_id: str = ""
    skills: List[SkillFact] = []
    domains: List[DomainFact] = []
    objectives: List[ObjectiveFact] = []
    offers: List[OfferFact] = []
    achievements: List[AchievementFact] = []
    topics: List[TopicFact] = []
    needs: List[NeedFact] = []
    assets: List[AssetFact] = []
    projects: List[ProjectFact] = []
    locations: List[LocationFact] = []
    constraints: List[ConstraintFact] = []
    privacy_labels: List[str] = []
    model_used: str = ""
    raw_response: str = ""
    class Config:
        extra = "ignore"


# ─────────────────────────────────────────────────────────────
#  API request / response models
# ─────────────────────────────────────────────────────────────

class ExtractionRequest(BaseModel):
    user_id: UUID
    tenant_id: UUID
    source_type: str = "cv"
    force_reextract: bool = False


class ExtractionResponse(BaseModel):
    document_id: UUID
    user_id: UUID
    facts_written: int = 0
    skills_found: int = 0
    domains_found: int = 0
    objectives_found: int = 0
    offers_found: int = 0
    achievements_found: int = 0
    topics_found: int = 0
    needs_found: int = 0
    assets_found: int = 0
    projects_found: int = 0
    locations_found: int = 0
    constraints_found: int = 0
    model_used: str = ""
    status: str = "pending"
    errors: List[str] = []
    ikg_errors: List[str] = []


class DocumentUploadResponse(BaseModel):
    document_id: str
    filename: str
    source_type: str
    status: str
    chunk_count: int
    storage_uri: str
    message: str


class DocumentDetailResponse(BaseModel):
    document_id: str
    filename: str
    source_type: str
    status: str
    storage_uri: Optional[str] = None
    chunk_count: int = 0
    extracted_facts: dict = {}


class ChunkOut(BaseModel):
    chunk_id: str
    chunk_index: int
    token_count: int
    text_preview: str