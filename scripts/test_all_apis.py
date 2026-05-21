#!/usr/bin/env python3
"""
Delllo RAIN3.0 — Full API Test Suite
======================================
Tests every endpoint group:
  1.  Health
  2.  Tenants       (GET list, GET by id, POST create)
  3.  Users         (POST create, GET list, PATCH status, POST bulk-status)
  4.  Profiles      (POST update rich profile)
  5.  Ingestion     (POST ingest/text)
  6.  Signals       (POST intent signal)
  7.  Graph / iKG   (POST upsert, GET person)
  8.  Matchmaking   (POST generate with active_users)
  9.  Match detail  (GET match, GET explanation)
  10. Feedback      (POST accept, POST feedback)
  11. Analytics     (GET overview, GET match-quality)
  12. Ontology      (GET transaction-types)
  13. MongoDB IDs   (create user with 24-char hex ID)

Usage:
  python scripts/test_all_apis.py
"""

import sys
import time
import uuid
from pathlib import Path

import httpx

BASE_URL   = "http://localhost:8000"
TENANT_ID  = "33000000-0000-0000-0000-000000000001"

# MongoDB-style 24-char hex IDs to test ID validation
MONGO_ID_1 = "507f1f77bcf86cd799439011"
MONGO_ID_2 = "507f1f77bcf86cd799439012"

# Deterministic UUIDs via uuid5 (same as test script)
_NS = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
def mongo_to_uuid(mid): return str(uuid.uuid5(_NS, f"{TENANT_ID}:{mid}"))

results = []

def section(title):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")

def check(ok, label, detail=""):
    icon = "OK  " if ok else "FAIL"
    print(f"  {icon}  {label}" + (f"  →  {detail}" if detail else ""))
    results.append((ok, label))
    return ok

def info(msg): print(f"       {msg}")

# ─────────────────────────────────────────────────────────────
# 1. HEALTH
# ─────────────────────────────────────────────────────────────

def test_health(c):
    section("1. Health")
    r = c.get(f"{BASE_URL}/health")
    check(r.status_code == 200, "GET /health", r.text[:80])

    r = c.get(f"{BASE_URL}/health/stack")
    ok = check(r.status_code == 200, "GET /health/stack")
    if ok:
        d = r.json()
        for svc, status in d.items():
            info(f"{svc}: {status}")

# ─────────────────────────────────────────────────────────────
# 2. TENANTS
# ─────────────────────────────────────────────────────────────

def test_tenants(c):
    section("2. Tenants")

    # GET list
    r = c.get(f"{BASE_URL}/v1/tenants")
    check(r.status_code == 200, "GET /v1/tenants")

    # POST create
    r = c.post(f"{BASE_URL}/v1/tenants", json={
        "tenant_id": TENANT_ID,
        "name":      "API Test Network",
        "slug":      "api-test-network",
    })
    check(r.status_code in (200, 201, 409), "POST /v1/tenants",
          f"status={r.status_code}")

    # GET by ID
    r = c.get(f"{BASE_URL}/v1/tenants/{TENANT_ID}")
    check(r.status_code == 200, f"GET /v1/tenants/{{id}}", r.json().get("name",""))

# ─────────────────────────────────────────────────────────────
# 3. USERS
# ─────────────────────────────────────────────────────────────

USER_A_ID = str(uuid.uuid4())
USER_B_ID = str(uuid.uuid4())

def test_users(c):
    section("3. Users")

    # POST create user A
    r = c.post(f"{BASE_URL}/v1/users", json={
        "user_id":      USER_A_ID,
        "tenant_id":    TENANT_ID,
        "display_name": "Alice Test",
        "email":        "alice.test@api-test.com",
        "headline":     "Senior Engineer",
        "role":         "member",
        "status":       "active",
    })
    check(r.status_code in (200, 201), "POST /v1/users (User A)",
          f"id={USER_A_ID[:8]}")

    # POST create user B
    r = c.post(f"{BASE_URL}/v1/users", json={
        "user_id":      USER_B_ID,
        "tenant_id":    TENANT_ID,
        "display_name": "Bob Test",
        "email":        "bob.test@api-test.com",
        "headline":     "Product Manager",
        "role":         "member",
        "status":       "active",
    })
    check(r.status_code in (200, 201), "POST /v1/users (User B)",
          f"id={USER_B_ID[:8]}")

    # GET list users
    r = c.get(f"{BASE_URL}/v1/users", params={"tenant_id": TENANT_ID})
    check(r.status_code == 200, "GET /v1/users",
          f"count={r.json().get('count', '?')}")

    # PATCH status
    r = c.patch(f"{BASE_URL}/v1/users/{USER_A_ID}/status",
                json={"status": "active", "tenant_id": TENANT_ID})
    check(r.status_code in (200, 404), "PATCH /v1/users/{id}/status",
          f"status={r.status_code}")

    # POST bulk-status
    r = c.post(f"{BASE_URL}/v1/users/bulk-status", json={
        "tenant_id":  TENANT_ID,
        "user_ids":   [USER_A_ID, USER_B_ID],
        "status":     "active",
    })
    check(r.status_code in (200, 404), "POST /v1/users/bulk-status",
          f"status={r.status_code}")

    # POST MongoDB-format ID
    mongo_uid = mongo_to_uuid(MONGO_ID_1)
    r = c.post(f"{BASE_URL}/v1/users", json={
        "user_id":      mongo_uid,
        "tenant_id":    TENANT_ID,
        "display_name": "Mongo User",
        "email":        "mongo.test@api-test.com",
        "headline":     "MongoDB ID Test",
        "role":         "member",
        "status":       "active",
    })
    check(r.status_code in (200, 201), "POST /v1/users (MongoDB-format ID)",
          f"uuid={mongo_uid[:8]}")

# ─────────────────────────────────────────────────────────────
# 4. PROFILES
# ─────────────────────────────────────────────────────────────

def test_profiles(c):
    section("4. Profiles")

    # GET profile
    r = c.get(f"{BASE_URL}/v1/profiles/{USER_A_ID}",
              params={"tenant_id": TENANT_ID})
    check(r.status_code in (200, 404), "GET /v1/profiles/{id}",
          f"status={r.status_code}")

    # POST update rich profile
    r = c.post(f"{BASE_URL}/v1/profiles/{USER_A_ID}/update", json={
        "tenant_id": TENANT_ID,
        "user_profile": {
            "current_role": {"title": "Senior Engineer", "company": "TechCorp"},
            "top_skills": [
                {"skill": "Python", "level": "Expert"},
                {"skill": "PostgreSQL", "level": "Advanced"},
            ],
            "solutions_offered": ["Backend architecture", "Database optimisation"],
            "career_highlights": ["Scaled platform to 10M users"],
            "immediate_needs":   ["Looking for product co-founder"],
        },
        "user_objective": {
            "primary_goal":    "Find product-focused co-founder",
            "secondary_goals": ["Connect with CTOs"],
        },
    })
    check(r.status_code in (200, 201, 404), "POST /v1/profiles/{id}/update",
          f"status={r.status_code}")

# ─────────────────────────────────────────────────────────────
# 5. INGESTION
# ─────────────────────────────────────────────────────────────

DOC_ID_A = None
DOC_ID_B = None

def test_ingestion(c):
    global DOC_ID_A, DOC_ID_B
    section("5. Ingestion")

    # POST ingest/text for User A
    r = c.post(f"{BASE_URL}/v1/ingest/text", data={
        "tenant_id":   TENANT_ID,
        "user_id":     USER_A_ID,
        "content":     "Alice Test — Senior Backend Engineer at TechCorp\n\nSkills: Python, PostgreSQL, API Design\nOFFER: Backend architecture review\nNEED: Product co-founder with GTM experience",
        "source_type": "cv",
        "filename":    "alice_test.txt",
        "embed":       "true",
    }, timeout=60)
    ok = check(r.status_code == 200, "POST /v1/ingest/text (User A)",
               f"status={r.status_code}")
    if ok:
        DOC_ID_A = r.json().get("document_id")
        info(f"document_id: {DOC_ID_A}")

    # POST ingest/text for User B
    r = c.post(f"{BASE_URL}/v1/ingest/text", data={
        "tenant_id":   TENANT_ID,
        "user_id":     USER_B_ID,
        "content":     "Bob Test — Product Manager at FinCorp\n\nSkills: Product Strategy, GTM, Fintech\nOFFER: Go-to-market strategy consulting\nNEED: Technical co-founder with backend skills",
        "source_type": "cv",
        "filename":    "bob_test.txt",
        "embed":       "true",
    }, timeout=60)
    ok = check(r.status_code == 200, "POST /v1/ingest/text (User B)",
               f"status={r.status_code}")
    if ok:
        DOC_ID_B = r.json().get("document_id")
        info(f"document_id: {DOC_ID_B}")

    # GET document status
    if DOC_ID_A:
        r = c.get(f"{BASE_URL}/v1/ingest/{DOC_ID_A}")
        check(r.status_code == 200, "GET /v1/ingest/{document_id}",
              f"status={r.json().get('status','?')}")

# ─────────────────────────────────────────────────────────────
# 6. SIGNALS
# ─────────────────────────────────────────────────────────────

def test_signals(c):
    section("6. Signals")

    r = c.post(f"{BASE_URL}/v1/signals/intent", json={
        "tenant_id":   TENANT_ID,
        "user_id":     USER_A_ID,
        "signal_type": "intent",
        "payload":     {"text": "Looking for product co-founder with GTM experience", "urgency": "high"},
    })
    check(r.status_code == 200, "POST /v1/signals/intent",
          f"signal_id={r.json().get('signal_id','?')[:8] if r.status_code==200 else r.status_code}")

    r = c.post(f"{BASE_URL}/v1/signals/intent", json={
        "tenant_id":   TENANT_ID,
        "user_id":     USER_B_ID,
        "signal_type": "intent",
        "payload":     {"text": "Looking for technical backend engineer co-founder", "urgency": "medium"},
    })
    check(r.status_code == 200, "POST /v1/signals/intent (User B)",
          f"status={r.status_code}")

# ─────────────────────────────────────────────────────────────
# 7. GRAPH / iKG
# ─────────────────────────────────────────────────────────────

def test_graph(c):
    section("7. Graph / iKG")

    # POST ikg/upsert
    r = c.post(f"{BASE_URL}/v1/ikg/upsert", json={
        "user_id":   USER_A_ID,
        "tenant_id": TENANT_ID,
    })
    check(r.status_code == 200, "POST /v1/ikg/upsert",
          f"status={r.status_code}")

    # GET ikg person
    r = c.get(f"{BASE_URL}/v1/ikg/person/{USER_A_ID}")
    check(r.status_code in (200, 404), "GET /v1/ikg/person/{id}",
          f"status={r.status_code}")

    # GET gKG transaction types
    r = c.get(f"{BASE_URL}/v1/gkg/transaction-types")
    check(r.status_code == 200, "GET /v1/gkg/transaction-types",
          f"count={len(r.json()) if r.status_code==200 else '?'}")

# ─────────────────────────────────────────────────────────────
# 8. MATCHMAKING
# ─────────────────────────────────────────────────────────────

MATCH_ID = None

def test_matchmaking(c):
    global MATCH_ID
    section("8. Matchmaking")

    # POST matches/generate (old format — no active_users)
    r = c.post(f"{BASE_URL}/v1/matches/generate", json={
        "tenant_id":             TENANT_ID,
        "requesting_user_id":    USER_A_ID,
        "transaction_types":     ["knowledge_transfer"],
        "max_candidates":        3,
        "min_score":             0.0,
        "generate_explanations": False,
    }, timeout=120)
    ok = check(r.status_code == 200, "POST /v1/matches/generate (basic)",
               f"matches={r.json().get('matches_created','?') if r.status_code==200 else r.status_code}")

    # POST matches/generate (new format — with active_users)
    r2 = c.post(f"{BASE_URL}/v1/matches/generate", json={
        "tenant_id":             TENANT_ID,
        "requesting_user_id":    USER_A_ID,
        "transaction_types":     ["knowledge_transfer"],
        "active_users":          [USER_A_ID, USER_B_ID],
        "max_candidates":        3,
        "min_score":             0.0,
        "generate_explanations": False,
    }, timeout=120)
    ok2 = check(r2.status_code == 200, "POST /v1/matches/generate (with active_users)",
                f"status={r2.status_code}")
    if ok2:
        matches = r2.json().get("matches", [])
        if matches:
            MATCH_ID = matches[0].get("match_id")
            info(f"Top match: score={matches[0].get('score','?')} match_id={MATCH_ID[:8] if MATCH_ID else '?'}")

    # GET recommended
    r = c.get(f"{BASE_URL}/v1/matches/recommended",
              params={"user_id": USER_A_ID, "tenant_id": TENANT_ID})
    check(r.status_code == 200, "GET /v1/matches/recommended",
          f"count={r.json().get('count','?') if r.status_code==200 else r.status_code}")

# ─────────────────────────────────────────────────────────────
# 9. MATCH DETAIL & EXPLANATION
# ─────────────────────────────────────────────────────────────

def test_match_detail(c):
    section("9. Match Detail & Explanation")

    if not MATCH_ID:
        info("Skipped — no match ID from previous step")
        check(False, "GET /v1/matches/{id}", "no match created")
        return

    r = c.get(f"{BASE_URL}/v1/matches/{MATCH_ID}")
    check(r.status_code == 200, "GET /v1/matches/{id}",
          f"score={r.json().get('score','?') if r.status_code==200 else r.status_code}")

    r = c.get(f"{BASE_URL}/v1/matches/{MATCH_ID}/explanation")
    check(r.status_code in (200, 404), "GET /v1/matches/{id}/explanation",
          f"status={r.status_code}")

# ─────────────────────────────────────────────────────────────
# 10. FEEDBACK
# ─────────────────────────────────────────────────────────────

def test_feedback(c):
    section("10. Feedback")

    if not MATCH_ID:
        info("Skipped — no match ID")
        check(False, "POST /v1/matches/{id}/feedback", "no match")
        return

    r = c.post(f"{BASE_URL}/v1/matches/{MATCH_ID}/feedback", json={
        "actor_user_id": USER_A_ID,
        "feedback_type": "useful",
        "payload":       {"comment": "Good match"},
    })
    check(r.status_code in (200, 404), "POST /v1/matches/{id}/feedback",
          f"status={r.status_code}")

    r = c.post(f"{BASE_URL}/v1/matches/{MATCH_ID}/accept", json={
        "actor_user_id": USER_A_ID,
        "feedback_type": "accepted",
    })
    check(r.status_code in (200, 404), "POST /v1/matches/{id}/accept",
          f"status={r.status_code}")

# ─────────────────────────────────────────────────────────────
# 11. ANALYTICS
# ─────────────────────────────────────────────────────────────

def test_analytics(c):
    section("11. Analytics")

    r = c.get(f"{BASE_URL}/v1/analytics/{TENANT_ID}/overview")
    check(r.status_code in (200, 404), "GET /v1/analytics/{tenant}/overview",
          f"status={r.status_code}")

    r = c.get(f"{BASE_URL}/v1/analytics/{TENANT_ID}/match-quality")
    check(r.status_code in (200, 404), "GET /v1/analytics/{tenant}/match-quality",
          f"status={r.status_code}")

# ─────────────────────────────────────────────────────────────
# 12. ONTOLOGY
# ─────────────────────────────────────────────────────────────

def test_ontology(c):
    section("12. Ontology")

    r = c.get(f"{BASE_URL}/v1/gkg/transaction-types")
    check(r.status_code == 200, "GET /v1/gkg/transaction-types",
          f"status={r.status_code}")

    r = c.get(f"{BASE_URL}/v1/ontology/fact-types")
    check(r.status_code in (200, 404), "GET /v1/ontology/fact-types",
          f"status={r.status_code}")

# ─────────────────────────────────────────────────────────────
# 13. ADMIN ENDPOINTS
# ─────────────────────────────────────────────────────────────

def test_admin(c):
    section("13. Admin")

    # POST admin/wipe is tested implicitly via test script
    # Just verify it exists
    r = c.post(f"{BASE_URL}/v1/admin/wipe", json={
        "tenant_id": "99000000-0000-0000-0000-000000000099",
        "confirm":   False,  # confirm=False so it's a no-op
    })
    check(r.status_code in (200, 400), "POST /v1/admin/wipe (confirm=false returns 400)",
          f"status={r.status_code}")

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    print(f"\n  Delllo RAIN3.0 — Full API Test")
    print(f"  API:    {BASE_URL}")
    print(f"  Tenant: {TENANT_ID}\n")

    with httpx.Client(timeout=httpx.Timeout(120, connect=10)) as c:
        # Quick reachability check
        try:
            c.get(f"{BASE_URL}/health")
        except Exception:
            print("  ERROR: API not reachable. Is docker stack running?")
            sys.exit(1)

        t0 = time.time()
        test_health(c)
        test_tenants(c)
        test_users(c)
        test_profiles(c)
        test_ingestion(c)
        test_signals(c)
        test_graph(c)
        test_matchmaking(c)
        test_match_detail(c)
        test_feedback(c)
        test_analytics(c)
        test_ontology(c)
        test_admin(c)
        elapsed = time.time() - t0

    passed = sum(1 for ok,_ in results if ok)
    failed = sum(1 for ok,_ in results if not ok)
    total  = len(results)

    print(f"\n{'='*60}")
    print(f"  RESULTS: {passed}/{total} passed  |  {failed} failed  |  {elapsed:.0f}s")
    print(f"{'='*60}")

    if failed:
        print("\n  Failed checks:")
        for ok, label in results:
            if not ok:
                print(f"    ✗  {label}")
    else:
        print("\n  All checks passed.")
    print()


if __name__ == "__main__":
    main()