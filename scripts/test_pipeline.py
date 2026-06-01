"""
Delllo RAIN3.0 — Full Matchmaking Test Pipeline (10 users)

Covers every route in the codebase:
  health · tenants · admin (users + wipe) · profiles · signals
  ingest (text + extract) · matches (generate / recommend / detail /
  accept / dismiss / feedback / explanation) · graph (ikg + gkg) ·
  analytics (overview / top-skills / match-quality / coverage / sweep)
  · ontology overrides

Usage:
  python test_pipeline.py                  # full run (Ollama extraction)
  python test_pipeline.py --skip-extract   # skip Ollama (fast smoke test)
  python test_pipeline.py --keep           # keep tenant data after run
  python test_pipeline.py --skip-extract --keep

Root-cause fixes applied vs original broken pipeline:
  1. Fixed deterministic TENANT_ID and user UUIDs (re-run safe)
  2. pre_wipe() before setup so re-runs always start clean
  3. create_tenant() runs BEFORE any user insert (FK-safe)
  4. Full error body printed on every failure
  5. --skip-extract flag so the pipeline runs without Ollama
"""

import json
import sys
from datetime import datetime, timezone

import httpx

# ─────────────────────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────────────────────

BASE_URL = "http://localhost:8000"
TIMEOUT  = 600.0

# ─────────────────────────────────────────────────────────────
#  Fixed deterministic IDs  (stable across re-runs)
# ─────────────────────────────────────────────────────────────

TENANT_ID = "a1b2c3d4-0000-0000-0000-000000000001"

USERS = [
    {
        "user_id":      "00000001-0000-0000-0000-000000000001",
        "display_name": "Alice Chen",
        "email":        "alice@delllo.test",
        "headline":     "Credit Quant Research",
        "role":         "member",
        "_profile": (
            "Alice is a credit quantitative researcher specialising in HY bond "
            "pricing, XVA and credit risk modelling using Python. "
            "She is based at Amsterdam HQ floor 7. "
            "She needs help with machine learning deployment and MLOps. "
            "She can mentor junior quants on credit risk models."
        ),
    },
    {
        "user_id":      "00000002-0000-0000-0000-000000000001",
        "display_name": "Bob Martinez",
        "email":        "bob@delllo.test",
        "headline":     "ML Platform Engineer",
        "role":         "member",
        "_profile": (
            "Bob is an ML Platform Engineer expert in machine learning deployment, "
            "MLOps, Kubernetes and Python. Based at Amsterdam HQ. "
            "He can help teams deploy ML models to production. "
            "He needs financial domain knowledge to better serve quant teams."
        ),
    },
    {
        "user_id":      "00000003-0000-0000-0000-000000000001",
        "display_name": "Clara Dubois",
        "email":        "clara@delllo.test",
        "headline":     "Regulatory Reporting Lead",
        "role":         "member",
        "_profile": (
            "Clara leads regulatory reporting covering Basel IV, FRTB and capital "
            "calculations. Located in the London Office. "
            "She can review regulatory capital approaches. "
            "She needs help with data pipeline automation and SQL optimisation."
        ),
    },
    {
        "user_id":      "00000004-0000-0000-0000-000000000001",
        "display_name": "David Okonkwo",
        "email":        "david@delllo.test",
        "headline":     "Data Engineering Lead",
        "role":         "member",
        "_profile": (
            "David is a data engineering lead skilled in Apache Spark, dbt and SQL. "
            "He is based in the London Office. "
            "He can build scalable data pipelines and automate reporting. "
            "He needs regulatory domain knowledge."
        ),
    },
    {
        "user_id":      "00000005-0000-0000-0000-000000000001",
        "display_name": "Eva Rossi",
        "email":        "eva@delllo.test",
        "headline":     "Treasury Risk Manager",
        "role":         "member",
        "_profile": (
            "Eva manages treasury risk including liquidity risk, ALM and interest "
            "rate risk. Based at Amsterdam HQ. "
            "She can advise on ALM strategy and treasury optimisation. "
            "She needs Python skills and data visualisation tools."
        ),
    },
    {
        "user_id":      "00000006-0000-0000-0000-000000000001",
        "display_name": "Felix Wagner",
        "email":        "felix@delllo.test",
        "headline":     "Senior Python Developer",
        "role":         "member",
        "_profile": (
            "Felix is a senior Python developer expert in data visualisation, "
            "Plotly, Dash and FastAPI. Located at Amsterdam HQ. "
            "He can build interactive dashboards and Python tooling. "
            "He needs financial product knowledge to serve the bank better."
        ),
    },
    {
        "user_id":      "00000007-0000-0000-0000-000000000001",
        "display_name": "Grace Kim",
        "email":        "grace@delllo.test",
        "headline":     "Equities Structuring VP",
        "role":         "member",
        "_profile": (
            "Grace is a VP in equities structuring covering equity derivatives and "
            "structured products. She is based in the Singapore Office. "
            "She can share deal structuring experience with junior bankers. "
            "She needs quantitative modelling and HY bond pricing knowledge."
        ),
    },
    {
        "user_id":      "00000008-0000-0000-0000-000000000001",
        "display_name": "Hiro Tanaka",
        "email":        "hiro@delllo.test",
        "headline":     "Quantitative Strategist Rates",
        "role":         "member",
        "_profile": (
            "Hiro is a quantitative strategist in rates covering interest rate "
            "derivatives. Expert in Python and C++. Singapore Office. "
            "He can review pricing model assumptions. "
            "He needs liquidity risk and regulatory reporting knowledge."
        ),
    },
    {
        "user_id":      "00000009-0000-0000-0000-000000000001",
        "display_name": "Isabelle Moreau",
        "email":        "isabelle@delllo.test",
        "headline":     "Chief Data Officer",
        "role":         "admin",
        "_profile": (
            "Isabelle is CDO with expertise in data strategy, AI governance and "
            "stakeholder management. Paris HQ. "
            "She can sponsor AI and data initiatives and remove organisational blockers. "
            "She needs MLOps and technical roadmap planning support."
        ),
    },
    {
        "user_id":      "00000010-0000-0000-0000-000000000001",
        "display_name": "James OBrien",
        "email":        "james@delllo.test",
        "headline":     "Technology Strategy Director",
        "role":         "member",
        "_profile": (
            "James is a technology strategy director skilled in cloud architecture "
            "and vendor management. London Office. "
            "He can align engineering priorities with business goals. "
            "He needs data strategy and AI governance guidance."
        ),
    },
]

REQUESTER = USERS[0]  # Alice — needs ML deployment help


# ─────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────

PASS = 0
FAIL = 0


def step(title: str) -> None:
    print(f"\n{'═'*62}")
    print(f"  {title}")
    print(f"{'═'*62}")


def check(label: str, resp: httpx.Response, expected: int) -> bool:
    global PASS, FAIL
    passed = resp.status_code == expected
    sym    = "✓" if passed else "✗"
    print(f"  {sym} [{resp.status_code}] {label}")
    if not passed:
        FAIL += 1
        try:
            body = resp.json()
            print(f"    └─ {json.dumps(body)[:500]}")
        except Exception:
            print(f"    └─ {resp.text[:500]}")
    else:
        PASS += 1
    return passed


def jget(resp: httpx.Response) -> dict:
    try:
        return resp.json()
    except Exception:
        return {}


def summary() -> None:
    total = PASS + FAIL
    print(f"\n{'═'*62}")
    print(f"  Results: {PASS}/{total} passed   {FAIL} failed")
    print(f"{'═'*62}\n")


# ─────────────────────────────────────────────────────────────
#  STEP 0 — Pre-wipe  (ensures re-runs always start clean)
# ─────────────────────────────────────────────────────────────

def pre_wipe(c: httpx.Client) -> None:
    step("STEP 0 — Pre-wipe (clean slate for re-runs)")
    r = c.post("/v1/admin/wipe", json={"tenant_id": TENANT_ID, "confirm": True})
    # 200 = wiped; any other code = tenant never existed yet — both are fine
    status = jget(r).get("status", "n/a")
    print(f"  wipe http={r.status_code}  status={status}")


# ─────────────────────────────────────────────────────────────
#  STEP 1 — Health
# ─────────────────────────────────────────────────────────────

def test_health(c: httpx.Client) -> None:
    step("STEP 1 — Health")
    check("GET /health",       c.get("/health"),       200)
    check("GET /health/live",  c.get("/health/live"),  200)
    check("GET /health/ready", c.get("/health/ready"), 200)
    r = c.get("/health/stack")
    check("GET /health/stack", r, 200)
    d = jget(r)
    svc = d.get("services", {})
    print(
        f"\n    overall={d.get('overall')}"
        f"  postgres={svc.get('postgres', {}).get('status')}"
        f"  memgraph={svc.get('memgraph', {}).get('status')}"
        f"  ollama={svc.get('ollama',   {}).get('status')}"
        f"  minio={svc.get('minio',    {}).get('status')}"
    )


# ─────────────────────────────────────────────────────────────
#  STEP 2 — Create tenant  (must happen before any user insert)
#
#  Root-cause of original 500s:
#    admin.py bootstraps the tenant with hardcoded slug='test-tenant'.
#    Second run: new UUID tries to INSERT with same slug →
#    ON CONFLICT DO NOTHING silently skips → FK violation on users.
#
#  Fix applied here: use a fixed TENANT_ID so the same row is
#  upserted every run (ON CONFLICT on tenant_id PK is harmless).
#
#  Fix to apply in admin.py (see BUGS section at bottom):
#    Change slug from 'test-tenant' to f"tenant-{tid[:8]}"
# ─────────────────────────────────────────────────────────────

def create_tenant(c: httpx.Client) -> None:
    step("STEP 2 — Create tenant (before any user FK reference)")

    # Trigger admin.py's auto-bootstrap path via a bootstrap user.
    # admin.py always inserts the tenant row before the user row.
    r = c.post("/v1/users", json={
        "user_id":      "b0000000-0000-0000-0000-000000000001",
        "tenant_id":    TENANT_ID,
        "display_name": "_bootstrap",
        "email":        "bootstrap@delllo.internal",
        "role":         "admin",
        "status":       "active",
    })

    if check("Bootstrap tenant via user insert", r, 201):
        print(f"\n    tenant {TENANT_ID[:8]}... created in tenants table ✓")
    else:
        print()
        print("    ── HINT ──────────────────────────────────────────────")
        print("    If you see 404 above, admin.router is not registered.")
        print("    Add this line to main.py:")
        print("      app.include_router(admin.router, prefix='/v1', tags=['admin'])")
        print("    ──────────────────────────────────────────────────────")
        raise SystemExit(1)


# ─────────────────────────────────────────────────────────────
#  STEP 3 — List tenants
# ─────────────────────────────────────────────────────────────

def test_tenants(c: httpx.Client) -> None:
    step("STEP 3 — Tenants")
    r = c.get("/v1/tenants")
    check("GET /v1/tenants", r, 200)
    tenants = jget(r).get("tenants", [])
    print(f"  {len(tenants)} tenant(s) in DB")

    r = c.get(f"/v1/tenants/{TENANT_ID}")
    check(f"GET /v1/tenants/{TENANT_ID[:8]}", r, 200)


# ─────────────────────────────────────────────────────────────
#  STEP 4 — Create 10 users
# ─────────────────────────────────────────────────────────────

def test_create_users(c: httpx.Client) -> None:
    step("STEP 4 — Create 10 users (POST /v1/users)")
    failures = 0
    for u in USERS:
        r = c.post("/v1/users", json={
            "user_id":      u["user_id"],
            "tenant_id":    TENANT_ID,
            "display_name": u["display_name"],
            "email":        u["email"],
            "headline":     u["headline"],
            "role":         u["role"],
            "status":       "active",
        })
        if not check(u["display_name"], r, 201):
            failures += 1

    r   = c.get(f"/v1/users?tenant_id={TENANT_ID}")
    check("GET /v1/users (list all)", r, 200)
    cnt = jget(r).get("count", 0)
    print(f"\n  users in DB: {cnt}  (failures={failures})")

    if cnt < 10:
        raise RuntimeError(
            f"Expected 10 users, got {cnt}. "
            "Fix errors above (likely admin.router not registered or admin.py slug bug)."
        )


# ─────────────────────────────────────────────────────────────
#  STEP 5 — Profile reads  (before extraction, expect empty facts)
# ─────────────────────────────────────────────────────────────

def test_profiles_empty(c: httpx.Client) -> None:
    step("STEP 5 — Profile reads (pre-extraction)")
    r = c.get(f"/v1/profiles/{REQUESTER['user_id']}")
    check("GET /v1/profiles/alice", r, 200)
    d = jget(r)
    print(f"  {d.get('display_name')} — {d.get('headline')}")

    r = c.get(f"/v1/profiles/{REQUESTER['user_id']}/facts")
    check("GET /v1/profiles/alice/facts (expect 0)", r, 200)
    print(f"  {len(jget(r).get('facts', []))} fact(s) (expected 0 before extraction)")

    # 404 for unknown user
    check("GET /v1/profiles/unknown-user (expect 404)",
          c.get("/v1/profiles/00000000-0000-0000-0000-000000000000"), 404)


# ─────────────────────────────────────────────────────────────
#  STEP 6 — Ingest text profiles
# ─────────────────────────────────────────────────────────────

def test_ingest_profiles(c: httpx.Client) -> dict:
    step("STEP 6 — Ingest text profiles (POST /v1/ingest/text)")
    doc_ids: dict[str, str] = {}

    for u in USERS:
        r = c.post(
            "/v1/ingest/text",
            data={
                "tenant_id":   TENANT_ID,
                "user_id":     u["user_id"],
                "content":     u["_profile"],
                "source_type": "bio",
                "filename":    u["display_name"].replace(" ", "_") + ".txt",
                "embed":       "false",   # skip pgvector embedding for speed
            },
        )
        d = jget(r)
        if check(f"Ingest text: {u['display_name']}", r, 200):
            doc_ids[u["user_id"]] = d.get("document_id", "")

    print(f"\n  ingested {len(doc_ids)}/10 documents")

    # Check document status for Alice
    alice_doc = doc_ids.get(REQUESTER["user_id"], "")
    if alice_doc:
        r = c.get(f"/v1/ingest/{alice_doc}?include_chunks=true")
        check(f"GET /v1/ingest/{alice_doc[:8]} (doc status)", r, 200)
        d = jget(r)
        print(f"  Alice doc: status={d.get('status')} chunks={d.get('chunk_count')}")

    return doc_ids


# ─────────────────────────────────────────────────────────────
#  STEP 7 — Extract facts  (requires Ollama; gracefully skipped)
# ─────────────────────────────────────────────────────────────

def test_extract_facts(c: httpx.Client, doc_ids: dict) -> None:
    step("STEP 7 — Extract facts via LLM (POST /v1/ingest/{doc_id}/extract)")
    print("  Requires Ollama — auto-skipped per-user if unavailable.\n")

    for u in USERS:
        doc_id = doc_ids.get(u["user_id"])
        if not doc_id:
            print(f"  ⚠  No doc_id for {u['display_name']} — skipping")
            continue

        r = c.post(
            f"/v1/ingest/{doc_id}/extract",
            json={
                "user_id":         u["user_id"],
                "tenant_id":       TENANT_ID,
                "source_type":     "bio",
                "force_reextract": False,
            },
            timeout=600.0,
        )
        d      = jget(r)
        status = d.get("status", "?")
        n      = d.get("facts_written", 0)
        errs   = d.get("errors", [])
        ikg_e  = d.get("ikg_errors", [])
        sym    = "✓" if status == "completed" else ("~" if status == "partial" else "✗")
        line   = (f"  {sym} {u['display_name']:<22} status={status:<10} "
                  f"facts={n:<4} "
                  f"skills={d.get('skills_found',0)} "
                  f"domains={d.get('domains_found',0)} "
                  f"needs={d.get('needs_found',0)} "
                  f"offers={d.get('offers_found',0)}")
        print(line)
        if errs:
            print(f"    pg_errors : {errs[:2]}")
        if ikg_e:
            print(f"    ikg_errors: {ikg_e[:2]}")


# ─────────────────────────────────────────────────────────────
#  STEP 8 — Profile facts  (post-extraction)
# ─────────────────────────────────────────────────────────────

def test_profiles_post_extract(c: httpx.Client) -> None:
    step("STEP 8 — Profile facts (post-extraction)")
    r = c.get(f"/v1/profiles/{REQUESTER['user_id']}/facts")
    check("GET /v1/profiles/alice/facts", r, 200)
    facts = jget(r).get("facts", [])
    print(f"  {len(facts)} fact(s) on Alice's profile")
    for f in facts[:8]:
        print(f"    [{f['fact_type']:12}] {str(f['canonical_value']):<35} "
              f"conf={f['confidence']:.2f}  vis={f['visibility']}")


# ─────────────────────────────────────────────────────────────
#  STEP 9 — iKG  (graph reads for Alice)
# ─────────────────────────────────────────────────────────────

def test_ikg(c: httpx.Client) -> None:
    step("STEP 9 — iKG graph reads")

    r = c.get(f"/v1/ikg/person/{REQUESTER['user_id']}")
    check("GET /v1/ikg/person/alice", r, 200)
    d = jget(r)
    print(
        f"  person node: {d.get('person', {}).get('display_name')}"
        f"  skills={len(d.get('skills', []))}"
        f"  domains={len(d.get('domains', []))}"
        f"  objectives={len(d.get('objectives', []))}"
        f"  offers={len(d.get('offers', []))}"
    )

    r = c.get(f"/v1/ikg/person/{REQUESTER['user_id']}/evidence")
    check("GET /v1/ikg/person/alice/evidence", r, 200)
    ev = jget(r).get("evidence", [])
    print(f"  {len(ev)} evidence node(s)")

    r = c.get(f"/v1/ikg/person/{REQUESTER['user_id']}/signals")
    check("GET /v1/ikg/person/alice/signals (pre-signal)", r, 200)

    # iKG upsert
    r = c.post("/v1/ikg/upsert", json={
        "user_id":   REQUESTER["user_id"],
        "tenant_id": TENANT_ID,
    })
    check("POST /v1/ikg/upsert (re-sync Alice)", r, 200)
    d = jget(r)
    print(f"  upsert: nodes_written={d.get('nodes_written')} "
          f"facts_processed={d.get('facts_processed')} "
          f"errors={len(d.get('errors', []))}")


# ─────────────────────────────────────────────────────────────
#  STEP 10 — gKG
# ─────────────────────────────────────────────────────────────

def test_gkg(c: httpx.Client) -> None:
    step("STEP 10 — gKG transaction types")
    r = c.get("/v1/gkg/transaction-types")
    check("GET /v1/gkg/transaction-types", r, 200)
    tts = jget(r).get("transaction_types", [])
    print(f"  {len(tts)} transaction type(s) in gKG")
    for tt in tts[:3]:
        print(f"    • {tt.get('type_id')} — {tt.get('name')}")

    # Rules for first type if any
    if tts:
        tid = tts[0].get("type_id", "")
        r = c.get(f"/v1/gkg/rules/{tid}")
        check(f"GET /v1/gkg/rules/{tid}", r, 200)
        d = jget(r)
        print(f"    requires={len(d.get('requires', []))} "
              f"boosts={len(d.get('boosts', []))}")


# ─────────────────────────────────────────────────────────────
#  STEP 11 — Live signals
# ─────────────────────────────────────────────────────────────

def test_signals(c: httpx.Client) -> None:
    step("STEP 11 — Live signals")

    # Alice: intent (needs ML help) + presence (Amsterdam HQ fl.7)
    check("Alice intent signal",
          c.post("/v1/signals/intent", json={
              "tenant_id":   TENANT_ID,
              "user_id":     REQUESTER["user_id"],
              "signal_type": "intent",
              "payload":     {
                  "text":    "Need ML deployment and MLOps help for credit risk model",
                  "urgency": "high",
              },
          }), 200)

    check("Alice presence — Amsterdam HQ fl.7",
          c.post("/v1/signals/presence", json={
              "tenant_id":   TENANT_ID,
              "user_id":     REQUESTER["user_id"],
              "signal_type": "presence",
              "payload":     {"location": "Amsterdam HQ", "floor": "7"},
          }), 200)

    # Bob: co-present at same floor (timing + proximity boost)
    check("Bob presence — Amsterdam HQ fl.7 (same as Alice)",
          c.post("/v1/signals/presence", json={
              "tenant_id":   TENANT_ID,
              "user_id":     USERS[1]["user_id"],
              "signal_type": "presence",
              "payload":     {"location": "Amsterdam HQ", "floor": "7"},
          }), 200)

    # Eva: availability window
    check("Eva availability signal",
          c.post("/v1/signals/availability", json={
              "tenant_id":   TENANT_ID,
              "user_id":     USERS[4]["user_id"],
              "signal_type": "availability",
              "payload":     {"available_until": "17:00", "mode": "in_person"},
          }), 200)

    # Verify Alice's signals appear in iKG after write
    r = c.get(f"/v1/ikg/person/{REQUESTER['user_id']}/signals")
    check("GET /v1/ikg/person/alice/signals (post-signal)", r, 200)
    d = jget(r)
    print(f"  intents={len(d.get('intents', []))}  presences={len(d.get('presences', []))}")


# ─────────────────────────────────────────────────────────────
#  STEP 12 — Generate matches
# ─────────────────────────────────────────────────────────────

def test_generate_matches(c: httpx.Client) -> list:
    step("STEP 12 — Generate match recommendations")

    r = c.post(
        "/v1/matches/generate",
        json={
            "tenant_id":             TENANT_ID,
            "requesting_user_id":    REQUESTER["user_id"],
            "transaction_types":     ["technical_problem_solving"],
            "max_candidates":        9,
            "min_score":             0.01,
            "generate_explanations": False,   # set True when Ollama is up
        },
        timeout=60.0,
    )
    check("POST /v1/matches/generate", r, 200)
    d = jget(r)

    matches = d.get("matches", [])
    print(
        f"\n  matches_created   = {d.get('matches_created', 0)}\n"
        f"  candidates_eval   = {d.get('candidates_evaluated', 0)}\n"
        f"  query_text        = '{d.get('query_text', '')[:70]}'\n"
        f"  score_version     = {d.get('score_version', '?')}"
    )

    if matches:
        print("\n  Ranked results:")
        hdr = f"  {'Name':<22} {'Score':>6}  {'rel':>5} {'comp':>5} {'time':>5} {'prox':>5} {'evid':>5} {'outc':>5} {'nov':>5}"
        print(hdr)
        print("  " + "─" * 58)
        for m in matches:
            bd = m.get("score_breakdown", {})
            print(
                f"  {m.get('candidate_name','?'):<22} "
                f"{m.get('score',0):>6.3f}  "
                f"{bd.get('relevance',0):>5.2f} "
                f"{bd.get('complementarity',0):>5.2f} "
                f"{bd.get('timing',0):>5.2f} "
                f"{bd.get('proximity',0):>5.2f} "
                f"{bd.get('evidence_strength',0):>5.2f} "
                f"{bd.get('outcome_likelihood',0):>5.2f} "
                f"{bd.get('novelty',0):>5.2f}"
            )
    else:
        print("\n  ⚠  No matches returned.")
        print("     This is expected when extraction was skipped (no extracted_facts")
        print("     → no candidates pass hard_filter → fallback pool empty).")
        print("     Run with Ollama up to get real facts and real matches.")

    return [m["match_id"] for m in matches]


# ─────────────────────────────────────────────────────────────
#  STEP 13 — GET recommended
# ─────────────────────────────────────────────────────────────

def test_get_recommended(c: httpx.Client) -> None:
    step("STEP 13 — GET recommended matches")
    r = c.get(
        "/v1/matches/recommended",
        params={
            "user_id":   REQUESTER["user_id"],
            "tenant_id": TENANT_ID,
            "limit":     10,
        },
    )
    check("GET /v1/matches/recommended", r, 200)
    recs = jget(r).get("recommended", [])
    print(f"  {len(recs)} recommendation(s) in response")
    for rec in recs[:3]:
        print(f"    • {rec.get('candidate_name','?'):<22} score={rec.get('score',0):.3f} "
              f"status={rec.get('status')}")


# ─────────────────────────────────────────────────────────────
#  STEP 14 — Match actions  (accept / dismiss / feedback)
# ─────────────────────────────────────────────────────────────

def test_match_actions(c: httpx.Client, match_ids: list) -> None:
    step("STEP 14 — Match actions")

    if not match_ids:
        print("  (no match_ids — skipping all action tests)")
        return

    mid0 = match_ids[0]

    # Accept top match
    check(f"Accept   {mid0[:8]}",
          c.post(f"/v1/matches/{mid0}/accept",
                 params={"actor_user_id": REQUESTER["user_id"]}), 200)

    # Idempotency guard — second accept must return 409
    check(f"Accept again (expect 409)",
          c.post(f"/v1/matches/{mid0}/accept",
                 params={"actor_user_id": REQUESTER["user_id"]}), 409)

    # Dismiss second match
    if len(match_ids) > 1:
        mid1 = match_ids[1]
        check(f"Dismiss  {mid1[:8]}",
              c.post(f"/v1/matches/{mid1}/dismiss",
                     params={"actor_user_id": REQUESTER["user_id"]}), 200)

    # Dismiss unknown match — must NOT 500 (BUG 7 guard)
    check("Dismiss unknown match_id (expect 404 not 500)",
          c.post("/v1/matches/00000000-dead-beef-0000-000000000000/dismiss",
                 params={"actor_user_id": REQUESTER["user_id"]}), 404)

    # Submit 'met' feedback on accepted match
    check("Feedback 'met' on match[0]",
          c.post(f"/v1/matches/{mid0}/feedback", json={
              "actor_user_id": REQUESTER["user_id"],
              "feedback_type": "met",
              "payload":       {"quality_score": 4, "notes": "Very helpful"},
          }), 200)

    # Submit 'not_useful' on third match
    if len(match_ids) > 2:
        check("Feedback 'not_useful' on match[2]",
              c.post(f"/v1/matches/{match_ids[2]}/feedback", json={
                  "actor_user_id": REQUESTER["user_id"],
                  "feedback_type": "not_useful",
                  "payload":       {},
              }), 200)

    # Invalid feedback_type must return 400
    check("Invalid feedback_type (expect 400)",
          c.post(f"/v1/matches/{mid0}/feedback", json={
              "actor_user_id": REQUESTER["user_id"],
              "feedback_type": "invalid_type",
              "payload":       {},
          }), 400)
# ─────────────────────────────────────────────────────────────
#  STEP 15 — Match detail + explanation
# ─────────────────────────────────────────────────────────────
def test_match_detail(c: httpx.Client, match_ids: list) -> None:
    step("STEP 15 — Match detail + explanation")

    if not match_ids:
        print("  (no match_ids — skipping)")
        return

    mid = match_ids[0]

    r = c.get(f"/v1/matches/{mid}")
    check(f"GET /v1/matches/{mid[:8]}", r, 200)
    d = jget(r)
    print(
        f"  {d.get('person_a_name')} → {d.get('person_b_name')}\n"
        f"  status={d.get('status')}  score={d.get('score')}\n"
        f"  tx_type={d.get('transaction_type')}  version={d.get('score_version')}"
    )
    bd = {k: d.get(k) for k in
          ["relevance","complementarity","timing","proximity",
           "evidence_strength","outcome_likelihood","novelty",
           "privacy_risk","interaction_friction"]}
    print(f"  breakdown: {bd}")

    # 404 for unknown match
    check("GET unknown match (expect 404)",
          c.get("/v1/matches/00000000-dead-beef-0000-000000000000"), 404)

    # Explanation (may be empty if Ollama was skipped)
    r = c.get(f"/v1/matches/{mid}/explanation")
    check(f"GET /v1/matches/{mid[:8]}/explanation", r, 200)
    expl = jget(r).get("explanation_text")
    if expl:
        print(f"\n  explanation: {expl[:120]}...")
    else:
        print("\n  (no explanation — Ollama not called)")


# ─────────────────────────────────────────────────────────────
#  STEP 16 — Meeting outcome signal
# ─────────────────────────────────────────────────────────────

def test_meeting_outcome(c: httpx.Client, match_ids: list) -> None:
    step("STEP 16 — Meeting outcome signal")

    if not match_ids:
        print("  (no match_ids — skipping)")
        return

    mid = match_ids[0]
    check("POST /v1/signals/meeting-outcome",
          c.post("/v1/signals/meeting-outcome", json={
              "tenant_id":   TENANT_ID,
              "user_id":     REQUESTER["user_id"],
              "signal_type": "meeting_outcome",
              "payload":     {
                  "match_id":      mid,
                  "met":           True,
                  "quality_score": 4,
                  "notes":         "Great session, very actionable.",
              },
          }), 200)


# ─────────────────────────────────────────────────────────────
#  STEP 17 — Analytics
# ─────────────────────────────────────────────────────────────

def test_analytics(c: httpx.Client) -> None:
    step("STEP 17 — Analytics")

    # Overview
    r = c.get(f"/v1/analytics/{TENANT_ID}/overview")
    check("GET /analytics/overview", r, 200)
    d = jget(r)
    m = d.get("matches", {})
    u = d.get("users",   {})
    f = d.get("facts",   {})
    print(
        f"  matches: total={m.get('total')} accepted={m.get('accepted')} "
        f"dismissed={m.get('dismissed')} avg_score={m.get('avg_score')}\n"
        f"  users:   total={u.get('total')} active={u.get('active')}\n"
        f"  facts:   users_with_facts={f.get('users_with_facts')} "
        f"total={f.get('total_facts')} avg_conf={f.get('avg_confidence')}\n"
        f"  feedback:{d.get('feedback', {})}"
    )

    # Top skills
    r = c.get(f"/v1/analytics/{TENANT_ID}/top-skills?limit=10")
    check("GET /analytics/top-skills", r, 200)
    by_type = jget(r).get("top_facts", {})
    for ft, items in by_type.items():
        names = [x["name"] for x in items[:4]]
        print(f"  top {ft}: {names}")

    # Match quality
    r = c.get(f"/v1/analytics/{TENANT_ID}/match-quality")
    check("GET /analytics/match-quality", r, 200)
    hist = jget(r).get("score_histogram", [])
    print(f"  score histogram: "
          + "  ".join(f"{h['bucket']}:{h['count']}" for h in hist))

    # Coverage
    r = c.get(f"/v1/analytics/{TENANT_ID}/coverage")
    check("GET /analytics/coverage", r, 200)
    d = jget(r)
    print(
        f"  coverage: cold_start_users={d.get('cold_start_users')} "
        f"no_signal_users={d.get('no_signal_users')}"
    )
    for row in d.get("users", [])[:4]:
        print(
            f"    {row.get('display_name','?'):<22} "
            f"facts={row.get('fact_count',0):>3}  "
            f"signals={row.get('active_signals',0)}  "
            f"open_matches={row.get('open_matches',0)}"
        )


# ─────────────────────────────────────────────────────────────
#  STEP 18 — Learning sweep
# ─────────────────────────────────────────────────────────────

def test_sweep(c: httpx.Client) -> None:
    step("STEP 18 — Learning sweep (POST /analytics/sweep)")
    r = c.post(f"/v1/analytics/{TENANT_ID}/sweep")
    check("POST /analytics/sweep", r, 200)
    d = jget(r)
    print(
        f"  users_processed={d.get('users_processed')} "
        f"snapshots_updated={d.get('snapshots_updated')} "
        f"errors={len(d.get('errors', []))}"
    )
    if d.get("errors"):
        for e in d["errors"][:3]:
            print(f"    ! {e}")


# ─────────────────────────────────────────────────────────────
#  STEP 19 — Ontology overrides
# ─────────────────────────────────────────────────────────────

def test_ontology(c: httpx.Client) -> None:
    step("STEP 19 — Ontology overrides")

    # List (initially empty)
    r = c.get(f"/v1/ontology/{TENANT_ID}/overrides")
    check("GET /ontology/overrides (empty)", r, 200)
    print(f"  existing overrides: {len(jget(r).get('overrides', []))}")

    # Create a weight_boost override
    r = c.post(f"/v1/ontology/{TENANT_ID}/overrides", json={
        "override_type":       "weight_boost",
        "transaction_type_id": "tt_technical_problem_solving",
        "target_capability":   "machine_learning",
        "weight_delta":        0.3,
        "reason":              "ML skills are critical for this tenant",
    })
    check("POST /ontology/overrides (weight_boost)", r, 201)
    override_id = jget(r).get("override_id", "")

    # Create a capability_block override
    r = c.post(f"/v1/ontology/{TENANT_ID}/overrides", json={
        "override_type":       "capability_block",
        "transaction_type_id": "tt_technical_problem_solving",
        "target_capability":   "legacy_mainframe",
        "reason":              "Not relevant for this tenant",
    })
    check("POST /ontology/overrides (capability_block)", r, 201)

    # Bad override_type → 400
    r = c.post(f"/v1/ontology/{TENANT_ID}/overrides", json={
        "override_type":       "invalid_type",
        "transaction_type_id": "tt_foo",
    })
    check("POST /ontology/overrides (bad type → 400)", r, 400)

    # Effective rules
    r = c.get(f"/v1/ontology/{TENANT_ID}/effective-rules/tt_technical_problem_solving")
    check("GET /ontology/effective-rules/tt_technical_problem_solving", r, 200)
    d = jget(r)
    print(f"  effective_rules={len(d.get('effective_rules', []))} "
          f"overrides_applied={d.get('overrides_applied', 0)}")

    # Delete the weight_boost override
    if override_id:
        r = c.delete(f"/v1/ontology/{TENANT_ID}/overrides/{override_id}")
        check(f"DELETE /ontology/overrides/{override_id[:8]}", r, 200)


# ─────────────────────────────────────────────────────────────
#  STEP 20 — Second match run  (re-run to test novelty penalty)
# ─────────────────────────────────────────────────────────────

def test_second_match_run(c: httpx.Client) -> None:
    step("STEP 20 — Second match run (novelty penalty visible in scores)")

    r = c.post(
        "/v1/matches/generate",
        json={
            "tenant_id":             TENANT_ID,
            "requesting_user_id":    REQUESTER["user_id"],
            "transaction_types":     ["technical_problem_solving"],
            "max_candidates":        9,
            "min_score":             0.01,
            "generate_explanations": False,
        },
        timeout=60.0,
    )
    check("POST /v1/matches/generate (2nd run)", r, 200)
    d = jget(r)
    matches = d.get("matches", [])
    print(f"  matches_created={d.get('matches_created',0)} "
          f"(dismissed matches should NOT re-appear)")
    for m in matches[:3]:
        bd = m.get("score_breakdown", {})
        print(f"    {m.get('candidate_name','?'):<22} score={m.get('score',0):.3f} "
              f"novelty={bd.get('novelty',0):.2f}")


# ─────────────────────────────────────────────────────────────
#  STEP 21 — Post-wipe  (cleanup)
# ─────────────────────────────────────────────────────────────

def post_wipe(c: httpx.Client) -> None:
    step("STEP 21 — Post-wipe (cleanup)")
    r = c.post("/v1/admin/wipe", json={"tenant_id": TENANT_ID, "confirm": True})
    check("POST /v1/admin/wipe", r, 200)
    d = jget(r)
    print(
        f"  tables_wiped   = {len(d.get('tables_wiped', []))}\n"
        f"  memgraph_wiped = {d.get('memgraph_wiped')}\n"
        f"  status         = {d.get('status')}\n"
        f"  errors         = {d.get('errors', [])[:3]}"
    )


# ─────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────

def run(skip_wipe: bool = False, skip_extract: bool = False) -> None:
    print(f"\n{'═'*62}")
    print("  Delllo RAIN3.0 — Full Matchmaking Pipeline Test")
    print(f"  Tenant      : {TENANT_ID}")
    print(f"  Requester   : {REQUESTER['display_name']}")
    print(f"  Time        : {datetime.now(timezone.utc).isoformat()}")
    print(f"  skip_extract: {skip_extract}   skip_wipe: {skip_wipe}")
    print(f"{'═'*62}")

    with httpx.Client(base_url=BASE_URL, timeout=TIMEOUT) as c:
        pre_wipe(c)                              # step 0
        test_health(c)                           # step 1
        create_tenant(c)                         # step 2  ← FK fix
        test_tenants(c)                          # step 3
        test_create_users(c)                     # step 4
        test_profiles_empty(c)                   # step 5
        doc_ids = test_ingest_profiles(c)        # step 6
        if not skip_extract:
            test_extract_facts(c, doc_ids)       # step 7  (needs Ollama)
        test_profiles_post_extract(c)            # step 8
        test_ikg(c)                              # step 9
        test_gkg(c)                              # step 10
        test_signals(c)                          # step 11
        match_ids = test_generate_matches(c)     # step 12
        test_get_recommended(c)                  # step 13
        test_match_actions(c, match_ids)         # step 14
        test_match_detail(c, match_ids)          # step 15
        test_meeting_outcome(c, match_ids)       # step 16
        test_analytics(c)                        # step 17
        test_sweep(c)                            # step 18
        test_ontology(c)                         # step 19
        test_second_match_run(c)                 # step 20
        if not skip_wipe:
            post_wipe(c)                         # step 21

    summary()


if __name__ == "__main__":
    run(
        skip_wipe    = "--keep"         in sys.argv,
        skip_extract = "--skip-extract" in sys.argv,
    )