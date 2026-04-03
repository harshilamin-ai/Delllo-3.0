import os
from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    environment: str = "development"
    log_level: str = "INFO"
    secret_key: str = "dev-secret-key"
    cors_origins: List[str] = ["http://localhost:3000", "http://localhost:5173"]

    database_url: str = "postgresql+asyncpg://delllo:delllo_secret@localhost:5432/delllo_db"
    memgraph_host: str = "localhost"
    memgraph_port: int = 7687
    memgraph_user: str = "memgraph"
    memgraph_password: str = "memgraph_secret"

    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minio_secret"
    minio_bucket_documents: str = "delllo-documents"
    minio_secure: bool = False

    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "qwen2.5:7b"

    weight_relevance: float = 0.24
    weight_complementarity: float = 0.16
    weight_timing: float = 0.14
    weight_evidence_strength: float = 0.14
    weight_outcome_likelihood: float = 0.10
    weight_proximity: float = 0.10
    weight_novelty: float = 0.06
    weight_interaction_friction: float = -0.06
    weight_privacy_risk: float = -0.04


settings = Settings()