# Delllo RAIN3.0 — Setup Guide

## Prerequisites
- Docker + Docker Compose
- Python 3.12+
- Ollama installed → https://ollama.com/download

---

## Step 1 — Clone & Configure

```bash
cp .env.example .env
# No changes needed for local dev — defaults work
```

---

## Step 2 — Start Infrastructure

```bash
docker compose up postgres memgraph minio prometheus grafana -d

# Verify all are healthy
docker compose ps
```

| Service    | URL                          | Purpose              |
|------------|------------------------------|----------------------|
| PostgreSQL | localhost:5432               | Relational + vectors |
| Memgraph   | localhost:3000 (Lab UI)      | Graph DB             |
| MinIO      | localhost:9001 (Console)     | Object storage       |
| Prometheus | localhost:9090               | Metrics              |
| Grafana    | localhost:3001               | Dashboards           |

---

## Step 3 — Init the Graph Schema

```bash
pip install -r requirements.txt
python scripts/init_graph.py
```

This creates all Memgraph constraints, indexes, and seeds the gKG ontology
(transaction types, problem types, capability types).

---

## Step 4 — Start Ollama with GPU

```bash
# Start the server (uses GPU automatically if available)
ollama serve

# In another terminal — pull the extraction model
ollama pull qwen2.5:7b

# Optional: pull the embedding model
ollama pull nomic-embed-text

# Verify
ollama list
```

---

## Step 5 — Start the API

```bash
# Option A: Direct (faster dev loop)
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Option B: Docker (matches production)
docker compose up api
```

API docs → http://localhost:8000/docs

---

## Step 6 — Run the Pipeline Test

```bash
python scripts/test_pipeline.py
```

Expected output:
```
✓ API is responding
✓ PostgreSQL healthy
✓ Memgraph healthy
✓ Ingestion returned 200
✓ Got 8 chunks
✓ Status = 'parsed'
✓ Facts were written to DB
```

---

## Key Endpoints

### Ingest a PDF CV
```bash
curl -X POST http://localhost:8000/v1/ingest/document \
  -F "file=@my_cv.pdf" \
  -F "tenant_id=00000000-0000-0000-0000-000000000002" \
  -F "user_id=00000000-0000-0000-0001-000000000003" \
  -F "source_type=cv"
```

### Run extraction on it
```bash
curl -X POST http://localhost:8000/v1/ingest/{document_id}/extract \
  -H "Content-Type: application/json" \
  -d '{
    "document_id": "{document_id}",
    "user_id": "00000000-0000-0000-0001-000000000003",
    "tenant_id": "00000000-0000-0000-0000-000000000002",
    "source_type": "cv"
  }'
```

### Full pipeline in one call
```bash
curl -X POST http://localhost:8000/v1/ingest/pipeline \
  -F "file=@my_cv.pdf" \
  -F "tenant_id=00000000-0000-0000-0000-000000000002" \
  -F "user_id=00000000-0000-0000-0001-000000000003" \
  -F "source_type=cv"
```

### Check stack health
```bash
curl http://localhost:8000/health/stack | python -m json.tool
```

---

## Project Structure

```
delllo/
├── app/
│   ├── main.py              ← FastAPI app entry point
│   ├── config.py            ← All settings (from .env)
│   ├── db/
│   │   ├── postgres.py      ← Async SQLAlchemy session
│   │   ├── graph.py         ← Memgraph neo4j driver
│   │   └── storage.py       ← MinIO client
│   ├── services/
│   │   ├── ingestion.py     ← Parse → Chunk → Embed → Store
│   │   └── extraction.py    ← LLM → JSON → extracted_facts
│   ├── routers/
│   │   ├── health.py        ← /health, /health/stack
│   │   ├── ingestion.py     ← /v1/ingest/*
│   │   ├── tenants.py       ← /v1/tenants
│   │   ├── profiles.py      ← /v1/profiles/*
│   │   ├── signals.py       ← /v1/signals/*
│   │   └── matches.py       ← /v1/matches/*  (Phase 1 stub)
│   └── schemas/
│       └── ingestion.py     ← Pydantic request/response models
├── scripts/
│   ├── init_db.sql          ← Full PostgreSQL schema
│   ├── init_graph.py        ← Memgraph schema + gKG seed
│   └── test_pipeline.py     ← End-to-end integration test
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```
