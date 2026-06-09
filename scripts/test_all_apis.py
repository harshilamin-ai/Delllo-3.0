#!/usr/bin/env python3
"""
Delllo RAIN3.0 — Full API Test Suite (Post Org/Network Migration)
"""

import sys
import time
import uuid
import argparse

import httpx

parser = argparse.ArgumentParser()
parser.add_argument("--url", default="http://localhost:8000", help="Base API URL")
args, _ = parser.parse_known_args()
BASE_URL = args.url.rstrip("/")

_NS       = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
MONGO_ID  = "507f1f77bcf86cd799439099"

def mongo_to_uuid(mid: str) -> str:
    return str(uuid.uuid5(_NS, mid))

RUN       = uuid.uuid4().hex[:6]
ORG_ID    = None
NET_ID    = None
RULE_ID   = None
USER_A_ID = str(uuid.uuid4())
USER_B_ID = str(uuid.uuid4())
USER_C_ID = str(uuid.uuid4())
MATCH_ID  = None
DOC_ID_A  = None

results: list[tuple[bool, str]] = []

def section(title: str):
    print(f"\n{'='*62}\n  {title}\n{'='*62}")

def check(ok: bool, label: str, detail: str = "") -> bool:
    icon = "OK  " if ok else "FAIL"
    print(f"  {icon}  {label}" + (f"  →  {detail}" if detail else ""))
    results.append((ok, label))
    return ok

def info(msg: str):
    print(f"       {msg}")

def skip(label: str, reason: str):
    print(f"  SKIP  {label}  →  {reason}")
    results.append((True, f"[SKIPPED] {label}"))


# ─────────────────────────────────────────────────────────────────
def test_health(c: httpx.Client):
    section("1. Health")
    r = c.get(f"{BASE_URL}/health")
    check(r.status_code == 200, "GET /health", r.text[:60])
    r = c.get(f"{BASE_URL}/health/stack")
    ok = check(r.status_code == 200, "GET /health/stack")
    if ok:
        d = r.json()
        for svc, status in d.get("services", {}).items():
            info(f"{svc}: {status.get('status','?')} — {status.get('detail','')[:60]}")
        info(f"overall: {d.get('overall','?')}  environment: {d.get('environment','?')}")
        info(f"ollama_model_target: {d.get('ollama_model_target','?')}")


# ─────────────────────────────────────────────────────────────────
def test_organisations(c: httpx.Client):
    global ORG_ID
    section("2. Organisations")

    slug = f"test-org-{RUN}"

    # POST — description is the right optional field (no "domain" on OrgCreate)
    r = c.post(f"{BASE_URL}/v1/organisations", json={
        "name":        f"Test Org {RUN}",
        "slug":        slug,
        "description": f"testorg-{RUN}.com",
    })
    ok = check(r.status_code == 201, "POST /v1/organisations",
               f"status={r.status_code}")
    if ok:
        ORG_ID = r.json()["org_id"]
        info(f"org_id: {ORG_ID[:8]}...")

    # Duplicate slug → 409
    r = c.post(f"{BASE_URL}/v1/organisations", json={"name": "Duplicate Org", "slug": slug})
    check(r.status_code == 409, "POST /v1/organisations (duplicate slug → 409)",
          f"status={r.status_code}")

    # List
    r = c.get(f"{BASE_URL}/v1/organisations")
    check(r.status_code == 200, "GET /v1/organisations",
          f"count={r.json().get('count', '?')}")

    if ORG_ID:
        # Get one
        r = c.get(f"{BASE_URL}/v1/organisations/{ORG_ID}")
        check(r.status_code == 200, "GET /v1/organisations/{org_id}",
              f"name={r.json().get('name','?')}")

        # Patch — OrgPatch accepts name, description, config (not "domain")
        r = c.patch(f"{BASE_URL}/v1/organisations/{ORG_ID}", json={
            "description": f"Updated description for {RUN}",
        })
        check(r.status_code == 200, "PATCH /v1/organisations/{org_id}",
              f"status={r.status_code}")

    # Unknown org → 404
    r = c.patch(f"{BASE_URL}/v1/organisations/{uuid.uuid4()}", json={"name": "x"})
    check(r.status_code == 404, "PATCH /v1/organisations (unknown org → 404)",
          f"status={r.status_code}")


# ─────────────────────────────────────────────────────────────────
def test_networks(c: httpx.Client):
    global NET_ID, RULE_ID
    section("3. Networks & Join Rules")

    if not ORG_ID:
        skip("all network tests", "no org_id from section 2")
        return

    net_slug = f"test-net-{RUN}"

    # Create network
    r = c.post(f"{BASE_URL}/v1/organisations/{ORG_ID}/networks", json={
        "name": f"Test Network {RUN}",
        "slug": net_slug,
    })
    ok = check(r.status_code == 201, "POST /v1/organisations/{org_id}/networks",
               f"status={r.status_code}")
    if ok:
        NET_ID = r.json()["network_id"]
        info(f"network_id: {NET_ID[:8]}...")

    # Duplicate slug → 409
    r = c.post(f"{BASE_URL}/v1/organisations/{ORG_ID}/networks", json={
        "name": "Dup Net", "slug": net_slug,
    })
    check(r.status_code == 409, "POST network (duplicate slug → 409)",
          f"status={r.status_code}")

    # Unknown org → 404
    r = c.post(f"{BASE_URL}/v1/organisations/{uuid.uuid4()}/networks", json={
        "name": "Ghost", "slug": f"ghost-{RUN}",
    })
    check(r.status_code == 404, "POST network (unknown org → 404)",
          f"status={r.status_code}")

    # List networks for org
    r = c.get(f"{BASE_URL}/v1/organisations/{ORG_ID}/networks")
    check(r.status_code == 200, "GET /v1/organisations/{org_id}/networks",
          f"count={r.json().get('count','?')}")

    if not NET_ID:
        skip("join rule tests", "no network_id")
        return

    # Add email_domain rule — field is "value" (not "rule_value")
    r = c.post(f"{BASE_URL}/v1/networks/{NET_ID}/rules", json={
        "rule_type": "email_domain",
        "value":     f"testorg-{RUN}.com",
    })
    ok = check(r.status_code == 201, "POST /v1/networks/{id}/rules (email_domain)",
               f"status={r.status_code}")
    if ok:
        RULE_ID = r.json()["rule_id"]
        info(f"rule_id: {RULE_ID[:8]}...")

    # Duplicate → 409
    r = c.post(f"{BASE_URL}/v1/networks/{NET_ID}/rules", json={
        "rule_type": "email_domain",
        "value":     f"testorg-{RUN}.com",
    })
    check(r.status_code == 409, "POST rule (duplicate email_domain → 409)",
          f"status={r.status_code}")

    # Missing value → 400
    r = c.post(f"{BASE_URL}/v1/networks/{NET_ID}/rules", json={"rule_type": "email_domain"})
    check(r.status_code == 400, "POST rule (email_domain missing value → 400)",
          f"status={r.status_code}")

    # Open rule
    r = c.post(f"{BASE_URL}/v1/networks/{NET_ID}/rules", json={"rule_type": "open"})
    check(r.status_code == 201, "POST /v1/networks/{id}/rules (open)",
          f"status={r.status_code}")

    # List rules
    r = c.get(f"{BASE_URL}/v1/networks/{NET_ID}/rules")
    check(r.status_code == 200, "GET /v1/networks/{id}/rules",
          f"count={r.json().get('count','?')}")

    # Delete rule
    if RULE_ID:
        r = c.delete(f"{BASE_URL}/v1/networks/{NET_ID}/rules/{RULE_ID}")
        check(r.status_code == 200, "DELETE /v1/networks/{id}/rules/{rule_id}",
              f"deleted={r.json().get('deleted','?')}")
        r = c.delete(f"{BASE_URL}/v1/networks/{NET_ID}/rules/{RULE_ID}")
        check(r.status_code == 404, "DELETE rule (already deleted → 404)",
              f"status={r.status_code}")

    # Members (empty at this point)
    r = c.get(f"{BASE_URL}/v1/networks/{NET_ID}/members")
    check(r.status_code == 200, "GET /v1/networks/{id}/members (empty)",
          f"count={r.json().get('count','?')}")


# ─────────────────────────────────────────────────────────────────
def test_tenants(c: httpx.Client):
    section("4. Tenants (org_id in responses)")

    # GET /v1/tenants — expects count key and org_id in each row
    r = c.get(f"{BASE_URL}/v1/tenants")
    ok = check(r.status_code == 200, "GET /v1/tenants",
               f"count={r.json().get('count','?')}")
    if ok and r.json().get("tenants"):
        first = r.json()["tenants"][0]
        check("org_id" in first, "org_id field present in tenant list",
              f"keys: {list(first.keys())}")

    # GET /v1/tenants/{tenant_id} — NET_ID is also a tenant_id
    if NET_ID:
        r = c.get(f"{BASE_URL}/v1/tenants/{NET_ID}")
        ok = check(r.status_code == 200, "GET /v1/tenants/{tenant_id}",
                   f"status={r.status_code}")
        if ok:
            check("org_id" in r.json(), "org_id field present in tenant detail",
                  f"org_id={r.json().get('org_id','missing')}")


# ─────────────────────────────────────────────────────────────────
def test_users(c: httpx.Client):
    section("5. Users")

    domain = f"testorg-{RUN}.com"

    # POST /v1/users — no tenant_id in body (global user registry)
    r = c.post(f"{BASE_URL}/v1/users", json={
        "user_id":      USER_A_ID,
        "display_name": "Alice Test",
        "email":        f"alice-{RUN}@{domain}",
        "headline":     "Senior Engineer",
        "status":       "active",
    })
    ok = check(r.status_code in (200, 201), "POST /v1/users (User A)",
               f"status={r.status_code}")
    if ok:
        body = r.json()
        # Response must NOT include tenant_id (users are no longer bound to a tenant at creation)
        check("tenant_id" not in body or body.get("tenant_id") is None,
              "POST /v1/users response has no tenant_id field",
              f"keys: {list(body.keys())}")

    r = c.post(f"{BASE_URL}/v1/users", json={
        "user_id":      USER_B_ID,
        "display_name": "Bob Test",
        "email":        f"bob-{RUN}@external-co.com",
        "headline":     "Product Manager",
        "status":       "active",
    })
    check(r.status_code in (200, 201), "POST /v1/users (User B, external domain)",
          f"status={r.status_code}")

    r = c.post(f"{BASE_URL}/v1/users", json={
        "user_id":      USER_C_ID,
        "display_name": "Charlie Test",
        "email":        f"charlie-{RUN}@external-co.com",
        "headline":     "Designer",
        "status":       "active",
    })
    check(r.status_code in (200, 201), "POST /v1/users (User C, for edge-case tests)",
          f"status={r.status_code}")

    # GET /v1/users — param is tenant_id (network_id = tenant_id in the DB)
    if NET_ID:
        r = c.get(f"{BASE_URL}/v1/users", params={"tenant_id": NET_ID})
        check(r.status_code == 200, "GET /v1/users?tenant_id=...",
              f"count={r.json().get('count','?')}")

    # PATCH status
    r = c.patch(f"{BASE_URL}/v1/users/{USER_A_ID}/status", json={"status": "active"})
    check(r.status_code in (200, 404), "PATCH /v1/users/{id}/status",
          f"status={r.status_code}")

    # Bulk status — field is tenant_id (not network_id)
    if NET_ID:
        r = c.post(f"{BASE_URL}/v1/users/bulk-status", json={
            "tenant_id": NET_ID,
            "user_ids":  [USER_A_ID, USER_B_ID],
            "status":    "active",
        })
        check(r.status_code == 200, "POST /v1/users/bulk-status",
              f"updated={r.json().get('users_updated','?')}")


# ─────────────────────────────────────────────────────────────────
def test_memberships(c: httpx.Client):
    section("6. Memberships")

    if not NET_ID:
        skip("all membership tests", "no network_id")
        return

    # GET network-suggestions — response key is "suggestions"
    r = c.get(f"{BASE_URL}/v1/users/{USER_A_ID}/network-suggestions")
    ok = check(r.status_code == 200, "GET /v1/users/{id}/network-suggestions",
               f"count={r.json().get('count','?')}")
    if ok:
        info(f"suggestions: {[s['name'] for s in r.json().get('suggestions', [])]}")

    # JOIN — User A has matching email domain + open rule → auto-approved
    r = c.post(f"{BASE_URL}/v1/users/{USER_A_ID}/join", json={"network_id": NET_ID})
    ok = check(r.status_code == 200, "POST /v1/users/{id}/join (User A, open → auto-approved)",
               f"status={r.json().get('status','?')}")
    if ok:
        check(r.json().get("status") == "active",
              "User A join status is 'active' (auto-approved via open rule)",
              f"status={r.json().get('status','?')}")

    # Idempotent — already a member
    r = c.post(f"{BASE_URL}/v1/users/{USER_A_ID}/join", json={"network_id": NET_ID})
    check(r.status_code == 200, "POST join (User A again → already member, 200)",
          f"message={r.json().get('message','?')[:40]}")

    # User B — external domain, no matching rule → pending
    r = c.post(f"{BASE_URL}/v1/users/{USER_B_ID}/join", json={"network_id": NET_ID})
    check(r.status_code == 200, "POST /v1/users/{id}/join (User B)",
          f"status={r.json().get('status','?')}")

    # User C — for reject test
    r = c.post(f"{BASE_URL}/v1/users/{USER_C_ID}/join", json={"network_id": NET_ID})
    check(r.status_code == 200, "POST /v1/users/{id}/join (User C, for reject test)",
          f"status={r.json().get('status','?')}")

    # Unknown user → 404
    r = c.post(f"{BASE_URL}/v1/users/{uuid.uuid4()}/join", json={"network_id": NET_ID})
    check(r.status_code == 404, "POST join (unknown user → 404)",
          f"status={r.status_code}")

    # Unknown network → 404
    r = c.post(f"{BASE_URL}/v1/users/{USER_A_ID}/join", json={"network_id": str(uuid.uuid4())})
    check(r.status_code == 404, "POST join (unknown network → 404)",
          f"status={r.status_code}")

    # Reject User C
    r_reject = c.post(f"{BASE_URL}/v1/networks/{NET_ID}/reject/{USER_C_ID}")
    check(r_reject.status_code in (200, 409), "POST /networks/{id}/reject/{user_id} (User C)",
          f"status={r_reject.status_code}")

    # Approve User B
    r = c.post(f"{BASE_URL}/v1/networks/{NET_ID}/approve/{USER_B_ID}")
    check(r.status_code in (200, 409), "POST /networks/{id}/approve/{user_id} (User B)",
          f"status={r.status_code}")

    # Unknown user → 404
    r = c.post(f"{BASE_URL}/v1/networks/{NET_ID}/approve/{uuid.uuid4()}")
    check(r.status_code == 404, "POST approve (unknown user → 404)",
          f"status={r.status_code}")

    # GET user networks — response key is "networks"
    r = c.get(f"{BASE_URL}/v1/users/{USER_A_ID}/networks")
    ok = check(r.status_code == 200, "GET /v1/users/{id}/networks",
               f"count={r.json().get('count','?')}")
    if ok:
        nets = r.json().get("networks", [])
        check(any(n["network_id"] == NET_ID for n in nets),
              "User A's network list includes test network")

    # Members after joins
    r = c.get(f"{BASE_URL}/v1/networks/{NET_ID}/members")
    ok = check(r.status_code == 200, "GET /v1/networks/{id}/members (after joins)",
               f"count={r.json().get('count','?')}")
    if ok:
        info(f"active members: {[m['display_name'] for m in r.json().get('members',[])]}")

    # Remove User B
    r = c.delete(f"{BASE_URL}/v1/networks/{NET_ID}/members/{USER_B_ID}")
    check(r.status_code in (200, 409), "DELETE /networks/{id}/members/{user_id} (User B)",
          f"status={r.status_code}")

    # Already removed → 404 or 409
    r = c.delete(f"{BASE_URL}/v1/networks/{NET_ID}/members/{USER_B_ID}")
    check(r.status_code in (404, 409), "DELETE member (already removed → 404/409)",
          f"status={r.status_code}")


# ─────────────────────────────────────────────────────────────────
def test_profiles(c: httpx.Client):
    section("7. Profiles")

    r = c.get(f"{BASE_URL}/v1/profiles/{USER_A_ID}")
    check(r.status_code in (200, 404), "GET /v1/profiles/{user_id}",
          f"status={r.status_code}")

    # POST profile update — tenant_id = NET_ID (network_id = tenant_id in matching engine)
    r = c.post(f"{BASE_URL}/v1/profiles/{USER_A_ID}/update", json={
        "tenant_id": NET_ID or "00000000-0000-0000-0000-000000000000",
        "user_profile": {
            "current_role":    {"title": "Senior Engineer", "company": "TechCorp"},
            "top_skills":      [
                {"skill": "Python",     "level": "Expert"},
                {"skill": "PostgreSQL", "level": "Intermediate"},
            ],
            "solutions_offered": ["Backend architecture", "DB optimisation"],
            "career_highlights": ["Scaled platform to 10M users"],
            "immediate_needs":   ["Looking for product co-founder"],
        },
        "user_objective": {
            "primary_goal":    "Find product-focused co-founder",
            "secondary_goals": ["Connect with CTOs"],
        },
    })
    check(r.status_code in (200, 201, 404), "POST /v1/profiles/{id}/update (User A)",
          f"facts={r.json().get('facts_written','?') if r.status_code==200 else r.status_code}")

    r = c.post(f"{BASE_URL}/v1/profiles/{USER_B_ID}/update", json={
        "tenant_id": NET_ID or "00000000-0000-0000-0000-000000000000",
        "user_profile": {
            "current_role":    {"title": "Product Manager", "company": "FinCorp"},
            "top_skills":      [{"skill": "GTM Strategy", "level": "Expert"}],
            "immediate_needs": ["Need technical co-founder"],
        },
    })
    check(r.status_code in (200, 201, 404), "POST /v1/profiles/{id}/update (User B)",
          f"status={r.status_code}")

    r = c.get(f"{BASE_URL}/v1/profiles/{USER_A_ID}/facts")
    check(r.status_code in (200, 404), "GET /v1/profiles/{id}/facts",
          f"count={r.json().get('count','?') if r.status_code==200 else r.status_code}")

    r = c.patch(f"{BASE_URL}/v1/profiles/{USER_A_ID}", json={
        "headline": "Senior Backend Engineer & Co-founder seeker",
    })
    check(r.status_code in (200, 404), "PATCH /v1/profiles/{user_id}",
          f"status={r.status_code}")


# ─────────────────────────────────────────────────────────────────
def test_ingestion(c: httpx.Client):
    global DOC_ID_A
    section("8. Ingestion")

    # tenant_id = NET_ID; user_id is UUID (no MongoDB conversion needed here)
    nid = NET_ID or "00000000-0000-0000-0000-000000000001"

    r = c.post(f"{BASE_URL}/v1/ingest/text", data={
        "tenant_id":   nid,
        "user_id":     USER_A_ID,
        "content":     (
            "Alice Test — Senior Backend Engineer\n\n"
            "Skills: Python, PostgreSQL, API Design\n"
            "OFFER: Backend architecture review\n"
            "NEED: Product co-founder with GTM experience"
        ),
        "source_type": "cv",
        "filename":    "alice_test.txt",
        "embed":       "true",
    }, timeout=60)
    ok = check(r.status_code == 200, "POST /v1/ingest/text (User A)",
               f"status={r.status_code}")
    if ok:
        DOC_ID_A = r.json().get("document_id")
        info(f"document_id: {DOC_ID_A}")

    r = c.post(f"{BASE_URL}/v1/ingest/text", data={
        "tenant_id":   nid,
        "user_id":     USER_B_ID,
        "content":     (
            "Bob Test — Product Manager\n\n"
            "Skills: Product Strategy, GTM, Fintech\n"
            "OFFER: Go-to-market strategy\n"
            "NEED: Technical co-founder with backend skills"
        ),
        "source_type": "cv",
        "filename":    "bob_test.txt",
        "embed":       "true",
    }, timeout=60)
    check(r.status_code == 200, "POST /v1/ingest/text (User B)",
          f"status={r.status_code}")

    if DOC_ID_A:
        r = c.get(f"{BASE_URL}/v1/ingest/{DOC_ID_A}")
        check(r.status_code == 200, "GET /v1/ingest/{document_id}",
              f"doc_status={r.json().get('status','?')}")


# ─────────────────────────────────────────────────────────────────
def test_signals(c: httpx.Client):
    section("9. Signals")

    # tenant_id = network_id (the matching engine tenant)
    nid = NET_ID or "00000000-0000-0000-0000-000000000001"

    r = c.post(f"{BASE_URL}/v1/signals/intent", json={
        "tenant_id":   nid,
        "user_id":     USER_A_ID,
        "signal_type": "intent",
        "payload":     {"text": "Looking for product co-founder", "urgency": "high"},
    })
    check(r.status_code == 200, "POST /v1/signals/intent (User A)",
          f"signal_id={str(r.json().get('signal_id','?'))[:8] if r.status_code==200 else r.status_code}")

    r = c.post(f"{BASE_URL}/v1/signals/intent", json={
        "tenant_id":   nid,
        "user_id":     USER_B_ID,
        "signal_type": "intent",
        "payload":     {"text": "Need technical backend co-founder", "urgency": "medium"},
    })
    check(r.status_code == 200, "POST /v1/signals/intent (User B)",
          f"status={r.status_code}")


# ─────────────────────────────────────────────────────────────────
def test_graph(c: httpx.Client):
    section("10. Graph / iKG")

    nid = NET_ID or "00000000-0000-0000-0000-000000000001"

    r = c.post(f"{BASE_URL}/v1/ikg/upsert", json={
        "user_id":   USER_A_ID,
        "tenant_id": nid,
    })
    check(r.status_code == 200, "POST /v1/ikg/upsert",
          f"status={r.status_code}")

    r = c.get(f"{BASE_URL}/v1/ikg/person/{USER_A_ID}")
    check(r.status_code in (200, 404), "GET /v1/ikg/person/{id}",
          f"status={r.status_code}")

    r = c.get(f"{BASE_URL}/v1/gkg/transaction-types")
    check(r.status_code == 200, "GET /v1/gkg/transaction-types",
          f"count={len(r.json().get('transaction_types',[])) if r.status_code==200 else '?'}")


# ─────────────────────────────────────────────────────────────────
def test_matchmaking(c: httpx.Client):
    global MATCH_ID
    section("11. Matchmaking")

    nid = NET_ID or "00000000-0000-0000-0000-000000000001"

    # Primary call — uses tenant_id (network_id = tenant_id)
    r = c.post(f"{BASE_URL}/v1/matches/generate", json={
        "tenant_id":             nid,
        "requesting_user_id":    USER_A_ID,
        "transaction_types":     ["knowledge_transfer"],
        "active_users":          [USER_A_ID, USER_B_ID],
        "max_candidates":        5,
        "min_score":             0.0,
        "generate_explanations": False,
    }, timeout=120)
    ok = check(r.status_code == 200, "POST /v1/matches/generate (tenant_id)",
               f"matches={r.json().get('matches_created','?') if r.status_code==200 else r.text[:60]}")
    if ok:
        matches = r.json().get("matches", [])
        if matches:
            MATCH_ID = matches[0].get("match_id")
            info(f"top match: score={matches[0].get('score','?')} match_id={str(MATCH_ID)[:8]}")

    # Back-compat: tenant_id alias also accepted
    r2 = c.post(f"{BASE_URL}/v1/matches/generate", json={
        "tenant_id":             nid,
        "requesting_user_id":    USER_A_ID,
        "transaction_types":     ["knowledge_transfer"],
        "max_candidates":        3,
        "min_score":             0.0,
        "generate_explanations": False,
    }, timeout=120)
    check(r2.status_code == 200, "POST /v1/matches/generate (tenant_id alias, back-compat)",
          f"status={r2.status_code}")

    # Non-member silently filtered via active_users
    ghost_id = str(uuid.uuid4())
    r3 = c.post(f"{BASE_URL}/v1/matches/generate", json={
        "tenant_id":             nid,
        "requesting_user_id":    USER_A_ID,
        "active_users":          [USER_A_ID, ghost_id],
        "max_candidates":        3,
        "min_score":             0.0,
        "generate_explanations": False,
    }, timeout=120)
    check(r3.status_code == 200, "POST /v1/matches/generate (non-member silently filtered)",
          f"status={r3.status_code}")

    # Recommended — param is user_id + tenant_id
    r = c.get(f"{BASE_URL}/v1/matches/recommended",
              params={"user_id": USER_A_ID, "tenant_id": nid})
    check(r.status_code == 200, "GET /v1/matches/recommended",
          f"count={r.json().get('count','?') if r.status_code==200 else r.status_code}")

    # Missing tenant_id/network_id → 422
    r = c.post(f"{BASE_URL}/v1/matches/generate", json={
        "requesting_user_id": USER_A_ID,
    }, timeout=10)
    check(r.status_code == 422, "POST /v1/matches/generate (no tenant_id → 422)",
          f"status={r.status_code}")


# ─────────────────────────────────────────────────────────────────
def test_match_detail(c: httpx.Client):
    section("12. Match Detail & Explanation")

    if not MATCH_ID:
        skip("GET /v1/matches/{id}", "no match created in section 11")
        skip("GET /v1/matches/{id}/explanation", "no match_id")
        return

    r = c.get(f"{BASE_URL}/v1/matches/{MATCH_ID}")
    check(r.status_code == 200, "GET /v1/matches/{match_id}",
          f"score={r.json().get('score','?') if r.status_code==200 else r.status_code}")

    r = c.get(f"{BASE_URL}/v1/matches/{MATCH_ID}/explanation")
    check(r.status_code in (200, 404), "GET /v1/matches/{match_id}/explanation",
          f"status={r.status_code}")


# ─────────────────────────────────────────────────────────────────
def test_feedback(c: httpx.Client):
    section("13. Feedback")

    if not MATCH_ID:
        skip("POST /v1/matches/{id}/feedback", "no match_id")
        skip("POST /v1/matches/{id}/accept",   "no match_id")
        return

    r = c.post(f"{BASE_URL}/v1/matches/{MATCH_ID}/feedback", json={
        "actor_user_id": USER_A_ID,
        "feedback_type": "useful",
        "payload":       {"comment": "Relevant connection"},
    })
    check(r.status_code in (200, 404), "POST /v1/matches/{id}/feedback",
          f"status={r.status_code}")

    # accept — actor_user_id as query param
    r = c.post(f"{BASE_URL}/v1/matches/{MATCH_ID}/accept",
               params={"actor_user_id": USER_A_ID})
    check(r.status_code in (200, 404, 409), "POST /v1/matches/{id}/accept",
          f"status={r.status_code}")


# ─────────────────────────────────────────────────────────────────
def test_analytics(c: httpx.Client):
    section("14. Analytics")

    nid = NET_ID or "00000000-0000-0000-0000-000000000001"

    r = c.get(f"{BASE_URL}/v1/analytics/{nid}/overview")
    check(r.status_code in (200, 404), "GET /v1/analytics/{tenant_id}/overview",
          f"status={r.status_code}")

    r = c.get(f"{BASE_URL}/v1/analytics/{nid}/match-quality")
    check(r.status_code in (200, 404), "GET /v1/analytics/{tenant_id}/match-quality",
          f"status={r.status_code}")


# ─────────────────────────────────────────────────────────────────
def test_ontology(c: httpx.Client):
    section("15. Ontology")

    r = c.get(f"{BASE_URL}/v1/gkg/transaction-types")
    check(r.status_code == 200, "GET /v1/gkg/transaction-types",
          f"status={r.status_code}")

    # Not yet implemented — accept 404
    r = c.get(f"{BASE_URL}/v1/ontology/fact-types")
    check(r.status_code in (200, 404), "GET /v1/ontology/fact-types",
          f"status={r.status_code}")


# ─────────────────────────────────────────────────────────────────
def test_admin(c: httpx.Client):
    section("16. Admin")

    r = c.post(f"{BASE_URL}/v1/admin/wipe", json={
        "tenant_id": NET_ID or "00000000-0000-0000-0000-000000000099",
        "confirm":   False,
    })
    check(r.status_code == 400, "POST /v1/admin/wipe (confirm=false → 400)",
          f"status={r.status_code}")


# ─────────────────────────────────────────────────────────────────
def test_edge_cases(c: httpx.Client):
    section("17. Edge Cases")

    # MongoDB ObjectID → deterministic UUID, create user with that UUID
    mongo_uuid = mongo_to_uuid(MONGO_ID)
    r = c.post(f"{BASE_URL}/v1/users", json={
        "user_id":      mongo_uuid,
        "display_name": "Mongo User",
        "email":        f"mongo-{RUN}@mongo-test.com",
        "headline":     "MongoDB ID Test",
        "status":       "active",
    })
    check(r.status_code in (200, 201), "POST /v1/users (MongoDB ObjectID → UUID)",
          f"uuid={mongo_uuid[:8]}")

    # Unknown org → 404
    r = c.get(f"{BASE_URL}/v1/organisations/{uuid.uuid4()}")
    check(r.status_code == 404, "GET /v1/organisations (unknown org → 404)",
          f"status={r.status_code}")

    # Patch with no fields → 400
    if ORG_ID:
        r = c.patch(f"{BASE_URL}/v1/organisations/{ORG_ID}", json={})
        check(r.status_code == 400, "PATCH /v1/organisations (no fields → 400)",
              f"status={r.status_code}")

    # Unknown network → 404
    r = c.get(f"{BASE_URL}/v1/networks/{uuid.uuid4()}/members")
    check(r.status_code == 404, "GET /networks members (unknown network → 404)",
          f"status={r.status_code}")

    # Unknown user → 404
    r = c.get(f"{BASE_URL}/v1/users/{uuid.uuid4()}/networks")
    check(r.status_code == 404, "GET /users/{id}/networks (unknown user → 404)",
          f"status={r.status_code}")

    # Missing required field → 422
    r = c.post(f"{BASE_URL}/v1/matches/generate", json={
        "requesting_user_id": USER_A_ID,
    }, timeout=10)
    check(r.status_code == 422, "POST /v1/matches/generate (no tenant_id → 422)",
          f"status={r.status_code}")


# ─────────────────────────────────────────────────────────────────
def main():
    print(f"\n  Delllo RAIN3.0 — Full API Test Suite")
    print(f"  API:  {BASE_URL}")
    print(f"  Run:  {RUN}")

    with httpx.Client(timeout=httpx.Timeout(120, connect=10)) as c:
        try:
            c.get(f"{BASE_URL}/health")
        except Exception:
            print("\n  ERROR: API is not reachable. Is the server running?\n")
            sys.exit(1)

        t0 = time.time()

        test_health(c)
        test_organisations(c)
        test_networks(c)
        test_tenants(c)
        test_users(c)
        test_memberships(c)
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
        test_edge_cases(c)

        elapsed = time.time() - t0

    passed  = sum(1 for ok, _ in results if ok)
    failed  = sum(1 for ok, _ in results if not ok)
    skipped = sum(1 for _, label in results if label.startswith("[SKIPPED]"))
    total   = len(results)

    print(f"\n{'='*62}")
    print(f"  RESULTS: {passed}/{total} passed  |  {failed} failed  |  {skipped} skipped  |  {elapsed:.1f}s")
    print(f"{'='*62}")

    if failed:
        print("\n  Failed checks:")
        for ok, label in results:
            if not ok:
                print(f"    ✗  {label}")
    else:
        print("\n  All checks passed.")
    print()

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()