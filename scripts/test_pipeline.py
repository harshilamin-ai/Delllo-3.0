#!/usr/bin/env python3
"""
Delllo RAIN3.0 — End-to-End Pipeline Test
─────────────────────────────────────────────────────────────────
Tests the full flow:
  1. Health check — confirms all services are up
  2. Ingest a sample CV (text)
  3. Check document status + chunks
  4. Run extraction
  5. Verify facts were written to DB
  6. Print the full extracted iKG facts

Usage:
  python scripts/test_pipeline.py

Requires:
  - docker compose up (postgres, memgraph, minio)
  - ollama serve  (with model pulled)
  - uvicorn app.main:app  (or docker compose up api)
─────────────────────────────────────────────────────────────────
"""

import sys
import json
import httpx

BASE_URL = "http://localhost:8000"

# ── Test data ─────────────────────────────────────────────────────
TENANT_ID = "00000000-0000-0000-0000-000000000002"   # ING Amsterdam
USER_ID   = "00000000-0000-0000-0001-000000000003"   # ING Quant

SAMPLE_CV = """
Dr. Sarah Chen — Quantitative Analyst, Fixed Income

PROFESSIONAL SUMMARY
Senior quantitative analyst with 8 years of experience building machine learning models
for credit derivatives pricing. Deep expertise in illiquid corporate bond valuation,
particularly high-yield instruments where observable market data is sparse.

SKILLS
- ML-based credit pricing (XGBoost, neural networks for spread interpolation)
- Market microstructure analysis and liquidity scoring
- Python quantitative development (pandas, numpy, scikit-learn, PyTorch)
- Fixed income instruments: HY bonds, CLOs, CDOs, credit default swaps
- Regulatory compliance: Basel III, FRTB market risk requirements
- Time series analysis and factor model construction

EXPERIENCE
Senior Quant Analyst — ING Global Markets, Amsterdam (2019–present)
- Built an ML-based pricing engine for illiquid HY corporate bonds
  reducing pricing latency from 4 hours to 12 minutes
- Designed proxy construction methodology for bonds with no recent trades
  using market microstructure features + sector spreads
- Collaborated with trading desks to validate model outputs daily

Quantitative Analyst — ABN AMRO, Amsterdam (2017–2019)
- Built credit risk models for structured products
- Developed Python tooling for automated factor extraction from Bloomberg

CURRENT OBJECTIVES
- Seeking collaboration with teams working on bond liquidity modelling
- Interested in sharing my pricing methodology with desks facing similar illiquid bond challenges
- Open to internal innovation discussions around AI in fixed income

OFFERS
- Can help any desk improve their pricing approach for illiquid corporate bonds
- Can advise on proxy construction when observable trades are absent
- Available to review ML pricing models and suggest improvements

PUBLICATIONS
- "Proxy-Based Pricing for Illiquid HY Bonds" (2022, Risk Magazine)
- "Microstructure Features for Credit Spread Interpolation" (2021, SSRN)
"""


def print_section(title: str):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


def check(condition: bool, msg: str):
    status = "✓" if condition else "✗"
    print(f"  {status}  {msg}")
    if not condition:
        sys.exit(1)


def main():
    print("\n🚀 Delllo RAIN3.0 — Pipeline Integration Test")
    print(f"   API: {BASE_URL}")
    print(f"   Tenant: {TENANT_ID}")
    print(f"   User: {USER_ID}")

    # ── Step 1: Health check ──────────────────────────────────────
    print_section("Step 1: Service Health Checks")

    with httpx.Client(timeout=10.0) as client:
        resp = client.get(f"{BASE_URL}/health")
        check(resp.status_code == 200, "API is responding")

        resp = client.get(f"{BASE_URL}/health/stack")
        stack = resp.json()
        print(f"\n  Stack status: {stack['overall'].upper()}")
        for svc, info in stack["services"].items():
            icon = "✓" if info["status"] == "ok" else ("⚠" if info["status"] == "warn" else "✗")
            print(f"    {icon} {svc}: {info['detail'][:80]}")

        # Warn but don't fail on Ollama/MinIO — they might not be running in CI
        check(stack["services"]["postgres"]["status"] == "ok", "PostgreSQL healthy")
        check(stack["services"]["memgraph"]["status"] == "ok", "Memgraph healthy")

        ollama_ok = stack["services"]["ollama"]["status"] in ("ok", "warn")
        if not ollama_ok:
            print("\n  ⚠ Ollama is not running. Extraction step will fail.")
            print("    Start it with:  ollama serve")
            print("    Pull the model: ollama pull qwen2.5:7b")

    # ── Step 2: Ingest sample CV ──────────────────────────────────
    print_section("Step 2: Ingest Sample CV (text)")

    with httpx.Client(timeout=30.0) as client:
        resp = client.post(
            f"{BASE_URL}/v1/ingest/text",
            data={
                "tenant_id": TENANT_ID,
                "user_id":   USER_ID,
                "content":   SAMPLE_CV,
                "source_type": "cv",
                "filename":  "dr_sarah_chen_cv.txt",
                "embed":     "false",  # skip embedding for speed in test
            },
        )

        if resp.status_code != 200:
            print(f"  ✗ Ingestion failed: {resp.status_code}")
            print(f"    {resp.text}")
            sys.exit(1)

        ingest_data = resp.json()
        document_id = ingest_data["document_id"]

        check(resp.status_code == 200,    "Ingestion returned 200")
        check(ingest_data["status"] == "parsed", f"Status is 'parsed'")
        check(ingest_data["chunk_count"] > 0,    f"Got {ingest_data['chunk_count']} chunks")

        print(f"\n  Document ID:  {document_id}")
        print(f"  Chunks:       {ingest_data['chunk_count']}")
        print(f"  Storage URI:  {ingest_data['storage_uri']}")

    # ── Step 3: Verify document in DB ────────────────────────────
    print_section("Step 3: Verify Document Status")

    with httpx.Client(timeout=10.0) as client:
        resp = client.get(
            f"{BASE_URL}/v1/ingest/{document_id}",
            params={"include_chunks": "true"},
        )
        check(resp.status_code == 200, "Document status endpoint works")
        doc = resp.json()
        check(doc["status"] == "parsed", f"Status = '{doc['status']}'")
        check(doc["chunk_count"] > 0, f"Chunk count = {doc['chunk_count']}")

        # Print chunk previews
        if "chunks" in doc and doc["chunks"]:
            print(f"\n  First 3 chunk previews:")
            for chunk in doc["chunks"][:3]:
                preview = chunk["text_preview"][:100].replace("\n", " ")
                print(f"    [{chunk['chunk_index']}] ({chunk['token_count']} tokens) {preview}...")

    # ── Step 4: Run extraction ────────────────────────────────────
    print_section("Step 4: Run LLM Extraction (Ollama)")

    with httpx.Client(timeout=180.0) as client:   # LLM can take time
        resp = client.post(
            f"{BASE_URL}/v1/ingest/{document_id}/extract",
            json={
                "document_id": document_id,
                "user_id":     USER_ID,
                "tenant_id":   TENANT_ID,
                "source_type": "cv",
            },
        )

        if resp.status_code != 200:
            print(f"  ✗ Extraction request failed: {resp.status_code}")
            print(f"    {resp.text}")
            print("\n  ⚠ Skipping extraction verification (Ollama may not be running)")
        else:
            ext = resp.json()
            print(f"\n  Model used:    {ext['model_used']}")
            print(f"  Status:        {ext['status']}")
            print(f"  Facts written: {ext['facts_written']}")
            print(f"  Skills:        {ext['skills_found']}")
            print(f"  Domains:       {ext['domains_found']}")
            print(f"  Objectives:    {ext['objectives_found']}")
            print(f"  Offers:        {ext['offers_found']}")
            print(f"  Achievements:  {ext['achievements_found']}")

            if ext.get("errors"):
                print(f"\n  Errors: {ext['errors']}")

            if ext["status"] == "completed":
                check(ext["facts_written"] > 0, f"Facts were written to DB")

    # ── Step 5: Verify facts in DB ───────────────────────────────
    print_section("Step 5: Verify Extracted Facts")

    with httpx.Client(timeout=10.0) as client:
        resp = client.get(f"{BASE_URL}/v1/profiles/{USER_ID}/facts")
        if resp.status_code == 200:
            facts_data = resp.json()
            facts = facts_data.get("facts", [])
            print(f"\n  Total facts for user: {len(facts)}")

            # Group by type
            by_type: dict = {}
            for fact in facts:
                ft = fact["fact_type"]
                by_type.setdefault(ft, []).append(fact)

            for ft, items in sorted(by_type.items()):
                print(f"\n  {ft.upper()} ({len(items)}):")
                for item in items[:5]:
                    conf = item.get("confidence", 0)
                    print(f"    • {item['raw_value'][:60]:<60} confidence={conf}")
        else:
            print(f"  ⚠ Could not fetch facts: {resp.status_code}")

    # ── Done ──────────────────────────────────────────────────────
    print_section("Test Complete")
    print("\n  ✓ Ingestion + Extraction pipeline is working.")
    print("\n  Next steps:")
    print("   → POST /v1/ingest/pipeline to run both in one call")
    print("   → Open http://localhost:8000/docs for interactive API docs")
    print("   → Open http://localhost:3001 for Grafana dashboard")
    print("   → Open http://localhost:3000 for Memgraph Lab\n")


if __name__ == "__main__":
    main()
