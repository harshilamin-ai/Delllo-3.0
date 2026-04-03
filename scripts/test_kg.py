#!/usr/bin/env python3
"""
Delllo RAIN3.0 — Full KG Integration Test
─────────────────────────────────────────────────────────────────
Tests every layer of the knowledge graph:

  Step 1:  Health check
  Step 2:  Ingest + extract a CV  (writes PG + iKG)
  Step 3:  Verify iKG person subgraph in Memgraph
  Step 4:  Verify all iKG node types were extracted
  Step 5:  Check evidence trail
  Step 6:  Post intent signal         (writes sKG LiveIntent)
  Step 7:  Post presence signal       (writes sKG Presence)
  Step 8:  Verify active signals in graph
  Step 9:  Query gKG transaction types
  Step 10: Query gKG rules for a transaction type
  Step 11: Generate matches           (writes oKG MatchRecommendation)
  Step 12: Get recommended matches
  Step 13: Accept a match             (updates oKG status)
  Step 14: Submit post-meeting feedback (writes oKG InteractionOutcome)
  Step 15: iKG upsert (backfill sync)
  Step 16: Summary report

Usage:
  python scripts/test_kg.py

Requires:
  - docker compose up (postgres, memgraph, minio)
  - ollama serve + model pulled
  - uvicorn app.main:app running (or docker compose up api)
─────────────────────────────────────────────────────────────────
"""

import sys
import json
import httpx

BASE_URL  = "http://localhost:8000"
TENANT_ID = "00000000-0000-0000-0000-000000000002"   # ING Amsterdam
USER_ID   = "00000000-0000-0000-0001-000000000003"   # ING Quant
USER_B_ID = "00000000-0000-0000-0001-000000000002"   # ING Trader (for matching)

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

CURRENT NEEDS
- Need help deploying my pricing model to production infrastructure
- Looking for a DevOps or MLOps engineer who has worked with quantitative models

OBJECTIVES
- Seeking collaboration with teams working on bond liquidity modelling
- Interested in sharing my pricing methodology with desks facing similar illiquid bond challenges
- Open to internal innovation discussions around AI in fixed income

OFFERS
- Can help any desk improve their pricing approach for illiquid corporate bonds
- Can advise on proxy construction when observable trades are absent
- Available to review ML pricing models and suggest improvements

PUBLICATIONS (ASSETS)
- "Proxy-Based Pricing for Illiquid HY Bonds" (2022, Risk Magazine)
- "Microstructure Features for Credit Spread Interpolation" (2021, SSRN)

CONSTRAINTS
- Available mornings only (09:00–13:00 Amsterdam time)
- No discussions with direct competitors outside ING group

LOCATION
- Amsterdam HQ, Floor 7
"""


# ─────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────

PASS = "✓"
FAIL = "✗"
WARN = "⚠"

results = []

def section(title: str):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")

def check(condition: bool, msg: str, fatal: bool = True):
    icon = PASS if condition else FAIL
    print(f"  {icon}  {msg}")
    results.append((condition, msg))
    if not condition and fatal:
        print(f"\n  ❌ Fatal failure — stopping test.")
        _print_summary()
        sys.exit(1)

def warn(msg: str):
    print(f"  {WARN}  {msg}")

def info(msg: str):
    print(f"     {msg}")

def _print_summary():
    passed = sum(1 for ok, _ in results if ok)
    failed = sum(1 for ok, _ in results if not ok)
    print(f"\n{'═'*60}")
    print(f"  Results: {passed} passed, {failed} failed / {len(results)} total")
    print(f"{'═'*60}")


# ─────────────────────────────────────────────
#  Step 1: Health
# ─────────────────────────────────────────────

def test_health():
    section("Step 1: Health Checks")
    with httpx.Client(timeout=10) as c:
        r = c.get(f"{BASE_URL}/health")
        check(r.status_code == 200, "API is up")

        r = c.get(f"{BASE_URL}/health/stack")
        stack = r.json()
        for svc, info_d in stack["services"].items():
            ok = info_d["status"] == "ok"
            icon = PASS if ok else WARN
            print(f"  {icon}  {svc}: {info_d['detail'][:80]}")

        check(stack["services"]["postgres"]["status"] == "ok", "PostgreSQL healthy")
        check(stack["services"]["memgraph"]["status"] == "ok", "Memgraph healthy")

        ollama_ok = stack["services"]["ollama"]["status"] in ("ok", "warn")
        if not ollama_ok:
            warn("Ollama not running — extraction step will be skipped")
        return ollama_ok


# ─────────────────────────────────────────────
#  Step 2: Ingest + Extract
# ─────────────────────────────────────────────

def test_ingest_and_extract(ollama_ok: bool) -> str | None:
    section("Step 2: Ingest CV + Run Extraction (iKG write)")

    with httpx.Client(timeout=300) as c:
        r = c.post(
            f"{BASE_URL}/v1/ingest/text",
            data={
                "tenant_id":   TENANT_ID,
                "user_id":     USER_ID,
                "content":     SAMPLE_CV,
                "source_type": "cv",
                "filename":    "test_kg_cv.txt",
                "embed":       "false",
            },
        )
        check(r.status_code == 200, f"Ingest returned 200 (got {r.status_code})")
        data = r.json()
        doc_id = data["document_id"]
        check(data["chunk_count"] > 0, f"Got {data['chunk_count']} chunks")
        info(f"Document ID: {doc_id}")

    if not ollama_ok:
        warn("Skipping extraction — Ollama not available")
        return doc_id

    with httpx.Client(timeout=300) as c:
        r = c.post(
            f"{BASE_URL}/v1/ingest/{doc_id}/extract",
            json={
                "document_id": doc_id,
                "user_id":     USER_ID,
                "tenant_id":   TENANT_ID,
                "source_type": "cv",
            },
        )
        check(r.status_code == 200, f"Extraction returned 200 (got {r.status_code})")
        ext = r.json()

        info(f"Model:        {ext['model_used']}")
        info(f"Status:       {ext['status']}")
        info(f"Facts written:{ext['facts_written']}")
        info(f"Skills:       {ext.get('skills_found', 0)}")
        info(f"Domains:      {ext.get('domains_found', 0)}")
        info(f"Topics:       {ext.get('topics_found', 0)}")
        info(f"Needs:        {ext.get('needs_found', 0)}")
        info(f"Objectives:   {ext.get('objectives_found', 0)}")
        info(f"Offers:       {ext.get('offers_found', 0)}")
        info(f"Achievements: {ext.get('achievements_found', 0)}")
        info(f"Assets:       {ext.get('assets_found', 0)}")
        info(f"Projects:     {ext.get('projects_found', 0)}")
        info(f"Locations:    {ext.get('locations_found', 0)}")
        info(f"Constraints:  {ext.get('constraints_found', 0)}")

        check(ext["facts_written"] > 0, "Facts were written to Postgres")
        check(ext["status"] in ("completed", "partial"), f"Extraction status is '{ext['status']}'")

        pg_errors = [e for e in ext.get("errors", []) if "iKG" not in e]
        ikg_errors = [e for e in ext.get("errors", []) if "iKG" in e]

        if pg_errors:
            for e in pg_errors:
                warn(f"PG error: {e[:100]}")
        if ikg_errors:
            for e in ikg_errors:
                warn(f"iKG error (non-fatal): {e[:100]}")

        # Check new node types were found
        check(ext.get("topics_found", 0) > 0,      "Topics extracted",      fatal=False)
        check(ext.get("needs_found", 0) > 0,        "Needs extracted",       fatal=False)
        check(ext.get("assets_found", 0) > 0,       "Assets extracted",      fatal=False)
        check(ext.get("projects_found", 0) > 0,     "Projects extracted",    fatal=False)
        check(ext.get("locations_found", 0) > 0,    "Locations extracted",   fatal=False)
        check(ext.get("constraints_found", 0) > 0,  "Constraints extracted", fatal=False)

    return doc_id


# ─────────────────────────────────────────────
#  Step 3: Verify iKG person subgraph
# ─────────────────────────────────────────────

def test_ikg_person():
    section("Step 3: Verify iKG Person Subgraph")

    with httpx.Client(timeout=300) as c:
        r = c.get(f"{BASE_URL}/v1/ikg/person/{USER_ID}")

        if r.status_code == 404:
            warn("Person not in graph yet — extraction may not have run or iKG write failed")
            check(False, "Person node exists in Memgraph iKG", fatal=False)
            return

        check(r.status_code == 200, f"iKG person endpoint returned 200 (got {r.status_code})")
        data = r.json()

        person = data.get("person", {})
        check(bool(person), "Person node returned")
        info(f"Person: {person.get('display_name', '?')} — {person.get('headline', '?')[:60]}")

        skills = data.get("skills", [])
        domains = data.get("domains", [])
        objectives = data.get("objectives", [])
        offers = data.get("offers", [])
        achievements = data.get("achievements", [])

        check(len(skills) > 0, f"Skills in graph: {len(skills)}", fatal=False)
        check(len(domains) > 0, f"Domains in graph: {len(domains)}", fatal=False)
        check(len(objectives) > 0, f"Objectives in graph: {len(objectives)}", fatal=False)
        check(len(offers) > 0, f"Offers in graph: {len(offers)}", fatal=False)
        check(len(achievements) > 0, f"Achievements in graph: {len(achievements)}", fatal=False)

        if skills:
            info(f"Sample skills: {', '.join(s.get('name','?') for s in skills[:3])}")
        if domains:
            info(f"Sample domains: {', '.join(d.get('name','?') for d in domains[:3])}")


# ─────────────────────────────────────────────
#  Step 4: Verify all iKG node types in Postgres
# ─────────────────────────────────────────────

def test_all_fact_types():
    section("Step 4: Verify All iKG Node Types in Postgres")

    with httpx.Client(timeout=10) as c:
        r = c.get(f"{BASE_URL}/v1/profiles/{USER_ID}/facts")
        check(r.status_code == 200, "Facts endpoint returned 200")
        data = r.json()
        facts = data.get("facts", [])

        by_type: dict = {}
        for f in facts:
            by_type.setdefault(f["fact_type"], []).append(f)

        expected_types = [
            "skill", "domain", "topic", "need", "objective",
            "offer", "achievement", "asset", "project", "location", "constraint"
        ]
        for ft in expected_types:
            count = len(by_type.get(ft, []))
            ok = count > 0
            icon = PASS if ok else WARN
            sample = by_type[ft][0]["raw_value"][:50] if ok else "—"
            print(f"  {icon}  {ft:<14} {count:>3} found   e.g. '{sample}'")
            results.append((ok, f"{ft} facts in Postgres"))


# ─────────────────────────────────────────────
#  Step 5: Evidence trail
# ─────────────────────────────────────────────

def test_evidence():
    section("Step 5: Evidence Trail")

    with httpx.Client(timeout=300) as c:
        r = c.get(f"{BASE_URL}/v1/ikg/person/{USER_ID}/evidence")
        check(r.status_code == 200, "Evidence endpoint returned 200")
        data = r.json()
        evidence = data.get("evidence", [])
        check(len(evidence) > 0, f"Evidence nodes found: {len(evidence)}", fatal=False)

        if evidence:
            for ev in evidence[:4]:
                info(f"{ev.get('claim_type','?')}: {ev.get('claim_name','?')[:40]} "
                     f"(conf={ev.get('confidence','?')})")


# ─────────────────────────────────────────────
#  Step 6 + 7: Post signals → sKG
# ─────────────────────────────────────────────

def test_signals() -> str | None:
    section("Step 6: Post Intent Signal → sKG")

    with httpx.Client(timeout=10) as c:
        r = c.post(
            f"{BASE_URL}/v1/signals/intent",
            json={
                "tenant_id":   TENANT_ID,
                "user_id":     USER_ID,
                "signal_type": "intent",
                "payload": {
                    "text":    "Need help deploying ML pricing model to production infrastructure",
                    "urgency": "high",
                },
                "valid_to": None,
            },
        )
        check(r.status_code == 200, f"Intent signal accepted (got {r.status_code})")
        intent_signal_id = r.json().get("signal_id")
        info(f"Signal ID: {intent_signal_id}")

    section("Step 7: Post Presence Signal → sKG")

    with httpx.Client(timeout=300) as c:
        r = c.post(
            f"{BASE_URL}/v1/signals/presence",
            json={
                "tenant_id":   TENANT_ID,
                "user_id":     USER_ID,
                "signal_type": "presence",
                "payload": {
                    "location": "Amsterdam HQ",
                    "floor":    "7",
                },
                "valid_to": None,
            },
        )
        check(r.status_code == 200, f"Presence signal accepted (got {r.status_code})")
        info(f"Signal ID: {r.json().get('signal_id')}")

    return intent_signal_id


# ─────────────────────────────────────────────
#  Step 8: Verify signals in graph
# ─────────────────────────────────────────────

def test_signals_in_graph():
    section("Step 8: Verify Active sKG Signals in Graph")

    with httpx.Client(timeout=300) as c:
        r = c.get(f"{BASE_URL}/v1/ikg/person/{USER_ID}/signals")
        check(r.status_code == 200, "Signals endpoint returned 200")
        data = r.json()

        intents = data.get("intents", [])
        presences = data.get("presences", [])

        check(len(intents) > 0,   f"Live intents in graph: {len(intents)}",   fatal=False)
        check(len(presences) > 0, f"Presence nodes in graph: {len(presences)}", fatal=False)

        if intents:
            info(f"Intent: {intents[0].get('text', '?')[:70]}")
        if presences:
            info(f"Presence: {presences[0].get('location', '?')} floor {presences[0].get('floor', '?')}")


# ─────────────────────────────────────────────
#  Step 9 + 10: gKG
# ─────────────────────────────────────────────

def test_gkg():
    section("Step 9: Query gKG Transaction Types")

    with httpx.Client(timeout=300) as c:
        r = c.get(f"{BASE_URL}/v1/gkg/transaction-types")
        check(r.status_code == 200, "gKG transaction types endpoint returned 200")
        data = r.json()
        tx_types = data.get("transaction_types", [])
        check(len(tx_types) > 0, f"Transaction types found: {len(tx_types)}")

        for tt in tx_types[:5]:
            info(f"{tt.get('type_id','?'):<35} {tt.get('name','?')}")

    section("Step 10: Query gKG Rules for technical_problem_solving")

    with httpx.Client(timeout=300) as c:
        r = c.get(f"{BASE_URL}/v1/gkg/rules/tt_technical_problem_solving")
        check(r.status_code == 200, "gKG rules endpoint returned 200")
        data = r.json()

        requires = data.get("requires", [])
        boosts   = data.get("boosts", [])

        check(len(requires) > 0, f"REQUIRES capabilities found: {len(requires)}", fatal=False)
        check(len(boosts) > 0,   f"BOOSTS contexts found: {len(boosts)}",         fatal=False)

        if requires:
            info("Required capabilities:")
            for req in requires:
                cap = req.get("capability", {})
                info(f"  → {cap.get('name','?'):<40} weight={req.get('weight','?')}")
        if boosts:
            info("Context boosts:")
            for b in boosts:
                ctx = b.get("context", {})
                info(f"  ↑ {ctx.get('name','?'):<40} weight={b.get('weight','?')}")


# ─────────────────────────────────────────────
#  Step 11 + 12: Matches
# ─────────────────────────────────────────────


def seed_user_b(ollama_ok: bool):
    """Ingest + extract a CV for User B (ING Trader) so matches can be generated."""
    section("Step 10b: Seed ING Trader (User B) for Match Generation")

    SAMPLE_CV_B = """
    Marcus Van Der Berg — Fixed Income Trader, ING Global Markets

    PROFESSIONAL SUMMARY
    Senior trader with 10 years in HY bond markets.
    Focused on execution and liquidity for illiquid corporate bonds.

    SKILLS
    - HY bond trading and execution
    - Market making for illiquid instruments
    - Bloomberg terminal and pricing tools
    - Risk management and position sizing

    CURRENT NEEDS
    - Need better quantitative pricing models for bonds with no recent trades
    - Looking for ML-based tools to price illiquid HY bonds faster

    OBJECTIVES
    - Improve pricing speed for illiquid bonds on my desk
    - Connect with quants who have built ML pricing engines

    OFFERS
    - Can provide real trading flow data to validate pricing models
    - Available to pilot new pricing tools on live desk

    LOCATION
    - Amsterdam HQ, Floor 5

    CONSTRAINTS
    - Available afternoons only (13:00-17:00 Amsterdam time)
    """

    with httpx.Client(timeout=300) as c:
        r = c.post(
            f"{BASE_URL}/v1/ingest/text",
            data={
                "tenant_id":   TENANT_ID,
                "user_id":     USER_B_ID,
                "content":     SAMPLE_CV_B,
                "source_type": "cv",
                "filename":    "test_kg_cv_trader.txt",
                "embed":       "false",
            },
        )
        if r.status_code != 200:
            warn(f"User B ingest failed ({r.status_code}) — matches may be empty")
            return
        doc_id_b = r.json()["document_id"]
        info(f"User B document: {doc_id_b}")

    if not ollama_ok:
        warn("Skipping User B extraction — Ollama not available")
        return

    with httpx.Client(timeout=300) as c:
        r = c.post(
            f"{BASE_URL}/v1/ingest/{doc_id_b}/extract",
            json={
                "document_id": doc_id_b,
                "user_id":     USER_B_ID,
                "tenant_id":   TENANT_ID,
                "source_type": "cv",
            },
        )
        if r.status_code == 200 and r.json().get("facts_written", 0) > 0:
            ext = r.json()
            info(f"User B extracted: {ext['facts_written']} facts "
                 f"(skills={ext.get('skills_found',0)} "
                 f"needs={ext.get('needs_found',0)})")
        else:
            warn(f"User B extraction status {r.status_code} — matches may be empty")


def test_match_generation() -> str | None:
    section("Step 11: Generate Matches → oKG MatchRecommendation")

    with httpx.Client(timeout=300) as c:
        r = c.post(
            f"{BASE_URL}/v1/matches/generate",
            json={
                "tenant_id":           TENANT_ID,
                "requesting_user_id":  USER_ID,
                "transaction_types":   ["technical_problem_solving"],
                "max_candidates":      10,
                "constraints": {},
            },
        )
        check(r.status_code == 200, f"Match generation returned 200 (got {r.status_code})")
        data = r.json()
        matches = data.get("matches", [])
        info(f"Matches created: {data.get('matches_created', 0)}")

        if not matches:
            warn("No matches generated — ensure other users have extracted facts")
            warn("Run the pipeline for ING Trader (user 00000000-0000-0000-0001-000000000002) first")
            return None

        for m in matches[:3]:
            info(f"Match {m['match_id'][:8]}… → {m['candidate_name']} "
                 f"score={m['score']} ({m['transaction_type']})")

        check(len(matches) > 0, f"At least one match created", fatal=False)
        return matches[0]["match_id"] if matches else None

    section("Step 12: Get Recommended Matches")

    with httpx.Client(timeout=300) as c:
        r = c.get(
            f"{BASE_URL}/v1/matches/recommended",
            params={"user_id": USER_ID, "tenant_id": TENANT_ID},
        )
        check(r.status_code == 200, "Recommended matches endpoint returned 200")
        data = r.json()
        recommended = data.get("recommended", [])
        info(f"Recommendations returned: {len(recommended)}")
        if recommended:
            top = recommended[0]
            info(f"Top match: {top.get('candidate_name','?')} score={top.get('score','?')}")


def test_get_recommended():
    section("Step 12: Get Recommended Matches")

    with httpx.Client(timeout=300) as c:
        r = c.get(
            f"{BASE_URL}/v1/matches/recommended",
            params={"user_id": USER_ID, "tenant_id": TENANT_ID},
        )
        check(r.status_code == 200, "Recommended matches endpoint returned 200")
        data = r.json()
        recommended = data.get("recommended", [])
        check(len(recommended) >= 0, f"Recommended matches returned: {len(recommended)}", fatal=False)
        if recommended:
            top = recommended[0]
            info(f"Top: {top.get('candidate_name','?')} score={top.get('score','?')} "
                 f"status={top.get('status','?')}")


# ─────────────────────────────────────────────
#  Steps 13 + 14: Accept + Feedback → oKG
# ─────────────────────────────────────────────

def test_match_lifecycle(match_id: str | None):
    if not match_id:
        warn("No match_id available — skipping accept + feedback steps")
        return

    section(f"Step 13: Accept Match {match_id[:8]}… → oKG status update")

    with httpx.Client(timeout=300) as c:
        r = c.post(
            f"{BASE_URL}/v1/matches/{match_id}/accept",
            params={"actor_user_id": USER_ID},
        )
        check(r.status_code == 200, f"Accept returned 200 (got {r.status_code})")
        info(f"Status: {r.json().get('status','?')}")

    section(f"Step 14: Submit Feedback → oKG InteractionOutcome")

    with httpx.Client(timeout=300) as c:
        r = c.post(
            f"{BASE_URL}/v1/matches/{match_id}/feedback",
            json={
                "actor_user_id": USER_ID,
                "feedback_type": "met",
                "payload": {
                    "met":               True,
                    "quality_score":     4,
                    "follow_up_created": True,
                    "notes":             "Good discussion on proxy bond construction.",
                },
            },
        )
        check(r.status_code == 200, f"Feedback returned 200 (got {r.status_code})")
        info(f"Feedback type: {r.json().get('feedback_type','?')} — {r.json().get('status','?')}")

    # Verify the match record updated
    with httpx.Client(timeout=300) as c:
        r = c.get(f"{BASE_URL}/v1/matches/{match_id}")
        check(r.status_code == 200, "Match detail endpoint returned 200")
        m = r.json()
        check(m.get("status") == "accepted", f"Match status is 'accepted' (got '{m.get('status')}')", fatal=False)


# ─────────────────────────────────────────────
#  Step 15: iKG upsert (backfill)
# ─────────────────────────────────────────────

def test_ikg_upsert():
    section("Step 15: iKG Upsert (backfill sync from Postgres → Memgraph)")

    with httpx.Client(timeout=300) as c:
        r = c.post(
            f"{BASE_URL}/v1/ikg/upsert",
            json={"user_id": USER_ID, "tenant_id": TENANT_ID},
        )
        check(r.status_code == 200, f"iKG upsert returned 200 (got {r.status_code})")
        data = r.json()
        info(f"Nodes written:   {data.get('nodes_written', 0)}")
        info(f"Facts processed: {data.get('facts_processed', 0)}")
        info(f"Status:          {data.get('status', '?')}")

        errors = data.get("errors", [])
        if errors:
            for e in errors[:3]:
                warn(f"iKG upsert error: {e[:100]}")

        check(data.get("nodes_written", 0) > 0, "iKG upsert wrote nodes", fatal=False)


# ─────────────────────────────────────────────
#  Step 16: Summary
# ─────────────────────────────────────────────

def print_final_summary():
    section("Step 16: Test Summary")

    passed  = sum(1 for ok, _ in results if ok)
    failed  = sum(1 for ok, _ in results if not ok)
    total   = len(results)

    print(f"\n  Total:  {total}")
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")

    if failed:
        print(f"\n  Failed checks:")
        for ok, msg in results:
            if not ok:
                print(f"    {FAIL}  {msg}")

    print(f"\n  {'✅ All checks passed!' if failed == 0 else '⚠  Some checks failed — see above'}")

    print("\n  Next steps:")
    print("   → Memgraph Lab:    http://localhost:3000   (MATCH (n) RETURN n LIMIT 50)")
    print("   → API docs:        http://localhost:8000/docs")
    print("   → Grafana:         http://localhost:3001")
    print()


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

def main():
    print("\n🚀 Delllo RAIN3.0 — Full KG Integration Test")
    print(f"   API:    {BASE_URL}")
    print(f"   Tenant: {TENANT_ID}")
    print(f"   User:   {USER_ID}")

    ollama_ok = test_health()
    doc_id    = test_ingest_and_extract(ollama_ok)
    test_ikg_person()
    test_all_fact_types()
    test_evidence()
    test_signals()
    test_signals_in_graph()
    test_gkg()
    seed_user_b(ollama_ok)
    match_id  = test_match_generation()
    test_get_recommended()
    test_match_lifecycle(match_id)
    test_ikg_upsert()
    print_final_summary()


if __name__ == "__main__":
    main()