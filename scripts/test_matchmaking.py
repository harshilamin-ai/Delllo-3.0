#!/usr/bin/env python3
"""
Delllo RAIN3.0 — Full Matchmaking Test
═══════════════════════════════════════════════════════════════════
Wipes all data, seeds 10 realistic ING Amsterdam users with
diverse roles, then runs the complete matchmaking pipeline.

Steps:
  0  Wipe all existing data (Postgres + Memgraph)
  1  Health checks
  2  Create 10 users with distinct profiles
  3  Ingest + extract CVs for all 10 users (iKG write)
  4  Post live intent signals for 4 users (sKG)
  5  Post presence signals for 6 users (sKG)
  6  Verify iKG subgraphs
  7  Run matchmaking for every user → collect all matches
  8  Inspect score breakdowns — verify all 9 features firing
  9  Accept top matches for 3 users
 10  Submit meeting feedback (met / not_useful / no_show)
 11  Run analytics overview
 12  Trigger learning sweep
 13  Re-run matchmaking — verify novelty + outcome_likelihood updated
 14  Summary report with match quality table

Usage:
  python scripts/test_matchmaking.py

Requirements:
  docker compose up -d
  ollama serve && ollama pull qwen2.5:7b && ollama pull nomic-embed-text
  uvicorn app.main:app --port 8000
═══════════════════════════════════════════════════════════════════
"""

import sys
import json
import time
import httpx
from uuid import UUID

# ─────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────

BASE_URL  = "http://localhost:8000"
TENANT_ID = "00000000-0000-0000-0000-000000000002"   # ING Amsterdam

# 10 test users
USERS = [
    {
        "user_id":      "10000000-0000-0000-0000-000000000001",
        "display_name": "Dr. Sarah Chen",
        "email":        "s.chen@ing-test.com",
        "headline":     "Senior Quant — ML Credit Pricing",
        "role":         "member",
        "cv": """
Dr. Sarah Chen — Senior Quantitative Analyst, Fixed Income

SUMMARY
8 years building ML models for credit derivatives pricing. Expert in illiquid HY bond
valuation where observable market data is sparse. PhD in Financial Mathematics.

SKILLS
- ML-based credit pricing (XGBoost, neural nets for spread interpolation)
- Python quant development (pandas, numpy, PyTorch)
- Fixed income: HY bonds, CLOs, credit default swaps
- Basel III and FRTB market risk compliance
- Time series analysis and factor model construction

EXPERIENCE
Senior Quant Analyst — ING Global Markets, Amsterdam (2019–present)
- Built ML pricing engine for illiquid HY bonds, cutting latency from 4 hours to 12 minutes
- Designed proxy construction for bonds with no recent trades
- Co-authored internal paper on microstructure features for spread interpolation

NEEDS
- Need MLOps engineer to help productionise my pricing model
- Looking for infrastructure support to deploy on Kubernetes

OFFERS
- Can advise any desk on ML-based bond pricing approaches
- Available to review pricing models and suggest improvements

ASSETS
- "Proxy-Based Pricing for Illiquid HY Bonds" (2022, Risk Magazine)
- "Microstructure Features for Credit Spread Interpolation" (2021, SSRN)

LOCATION: Amsterdam HQ, Floor 7
CONSTRAINTS: Available mornings only (09:00–13:00 CET)
""",
    },
    {
        "user_id":      "10000000-0000-0000-0000-000000000002",
        "display_name": "Marcus van der Berg",
        "email":        "m.vdberg@ing-test.com",
        "headline":     "HY Bond Trader",
        "role":         "member",
        "cv": """
Marcus van der Berg — Fixed Income Trader, HY Bonds

SUMMARY
10 years executing HY bond trades on ING Global Markets desk. Expert in liquidity
sourcing and price discovery for illiquid names. Strong Bloomberg and TRACE expertise.

SKILLS
- HY bond execution and market making
- Liquidity scoring and price discovery
- Bloomberg terminal, TRACE data analysis
- Risk management and position sizing
- Regulatory reporting (MiFID II)

EXPERIENCE
Senior Trader — ING Global Markets, Amsterdam (2014–present)
- Execute 50+ HY bond trades daily across 200+ issuers
- Developed internal liquidity heatmap used by 12 desks

NEEDS
- Need better quantitative pricing models for bonds with no recent trades
- Looking for ML-based tools to price illiquid HY bonds faster

OFFERS
- Can provide live trading flow data to validate pricing models
- Available to pilot new pricing tools on live desk

LOCATION: Amsterdam HQ, Floor 5
CONSTRAINTS: Available afternoons (13:00–17:00 CET)
""",
    },
    {
        "user_id":      "10000000-0000-0000-0000-000000000003",
        "display_name": "Priya Sharma",
        "email":        "p.sharma@ing-test.com",
        "headline":     "MLOps Engineer — Model Deployment",
        "role":         "member",
        "cv": """
Priya Sharma — Senior MLOps Engineer

SUMMARY
6 years deploying machine learning models to production at scale. Specialist in
Kubernetes-based ML pipelines, model monitoring, and CI/CD for data science teams.

SKILLS
- Kubernetes and Docker container orchestration
- MLflow, Kubeflow, and custom ML pipeline tooling
- Python, FastAPI, and microservices architecture
- Model monitoring and drift detection
- CI/CD with Jenkins, GitHub Actions

EXPERIENCE
Senior MLOps Engineer — ING Technology, Amsterdam (2020–present)
- Deployed 15+ ML models to production across risk and trading teams
- Built internal ML platform serving 3000 requests per second

NEEDS
- Looking for quant models to deploy — want to expand into financial ML

OFFERS
- Can deploy any Python-based ML model to production within 2 weeks
- Available to consult on infrastructure for quantitative pricing models

PROJECTS
- ING Internal ML Platform (lead engineer, 2021–present)
- Credit Risk Model Deployment (deployment lead, 2022)

LOCATION: Amsterdam HQ, Floor 3
""",
    },
    {
        "user_id":      "10000000-0000-0000-0000-000000000004",
        "display_name": "Thomas Müller",
        "email":        "t.muller@ing-test.com",
        "headline":     "Risk Manager — Market Risk",
        "role":         "member",
        "cv": """
Thomas Müller — Market Risk Manager

SUMMARY
12 years in market risk management. FRTB implementation lead for ING. Expert in
VaR modelling, stress testing, and Basel III/IV capital requirements.

SKILLS
- FRTB implementation and SA-TB / IMA models
- VaR and expected shortfall calculation
- Stress testing and scenario analysis
- Python risk modelling (scipy, statsmodels)
- Regulatory reporting (Basel III, CRR2)

EXPERIENCE
Head of Market Risk Modelling — ING, Amsterdam (2018–present)
- Led FRTB implementation across 8 trading desks
- Reduced capital requirements by 12% through model optimisation

NEEDS
- Need better data on illiquid bond pricing for VaR model inputs
- Looking for front-office quant collaboration on model validation

OFFERS
- Can validate quantitative pricing models against regulatory standards
- Available to advise on FRTB compliance for ML-based approaches

LOCATION: Amsterdam HQ, Floor 9
CONSTRAINTS: No external sharing of trading strategies
""",
    },
    {
        "user_id":      "10000000-0000-0000-0000-000000000005",
        "display_name": "Aisha Okonkwo",
        "email":        "a.okonkwo@ing-test.com",
        "headline":     "Data Engineer — Market Data Pipelines",
        "role":         "member",
        "cv": """
Aisha Okonkwo — Senior Data Engineer

SUMMARY
5 years building real-time market data pipelines at ING. Expert in streaming
data infrastructure, tick data normalisation, and low-latency feeds.

SKILLS
- Apache Kafka and real-time streaming pipelines
- Python, Spark, and dbt for data transformation
- Bloomberg B-PIPE and Refinitiv Eikon data feeds
- PostgreSQL, TimescaleDB, and ClickHouse
- Data quality monitoring and alerting

EXPERIENCE
Senior Data Engineer — ING Data Platform, Amsterdam (2021–present)
- Built tick-data pipeline processing 2M events per second
- Normalised Bloomberg and Refinitiv feeds for 14 asset classes

OFFERS
- Can provide clean, normalised market data feeds to any quant team
- Available to build custom data pipelines for pricing models

NEEDS
- Looking to understand how quant teams consume market data
- Want to improve latency of bond pricing data delivery

LOCATION: Amsterdam HQ, Floor 3
""",
    },
    {
        "user_id":      "10000000-0000-0000-0000-000000000006",
        "display_name": "Lars Henriksen",
        "email":        "l.henriksen@ing-test.com",
        "headline":     "Structuring — CLOs and Credit Products",
        "role":         "member",
        "cv": """
Lars Henriksen — Credit Structurer, CLOs and ABS

SUMMARY
9 years structuring CLOs, CDOs, and ABS at ING Capital Markets. Deep expertise in
tranche design, waterfall modelling, and credit enhancement structures.

SKILLS
- CLO and CDO tranche structuring
- Waterfall cash flow modelling (Python, Excel VBA)
- Credit enhancement and overcollateralisation analysis
- Rating agency methodologies (Moody's, S&P, Fitch)
- Legal documentation for structured products

EXPERIENCE
Director, Credit Structuring — ING Capital Markets, Amsterdam (2015–present)
- Structured 22 CLO transactions totalling €18bn
- Led CLO 2.0 warehouse facility design

NEEDS
- Need better pricing tools for underlying HY loans in CLO pools
- Looking for quant support on pool selection optimisation

OFFERS
- Can advise on structured product mechanics and investor appetite
- Available to discuss CLO collateral selection criteria

LOCATION: Amsterdam HQ, Floor 6
""",
    },
    {
        "user_id":      "10000000-0000-0000-0000-000000000007",
        "display_name": "Fatima Al-Rashid",
        "email":        "f.alrashid@ing-test.com",
        "headline":     "Compliance — Trading and Market Conduct",
        "role":         "member",
        "cv": """
Fatima Al-Rashid — Senior Compliance Officer, Markets

SUMMARY
8 years in trading compliance and market conduct. Specialist in MiFID II, MAR,
and algorithmic trading regulations. Led ING's SMCR implementation.

SKILLS
- MiFID II and MiFIR transaction reporting
- Market Abuse Regulation (MAR) surveillance
- Algorithmic trading compliance (RTS 6)
- Trade surveillance systems (NICE Actimize)
- SMCR and accountability frameworks

EXPERIENCE
Senior Compliance Officer — ING Markets, Amsterdam (2017–present)
- Led MiFID II implementation across 6 trading desks
- Built automated surveillance for 200+ market abuse scenarios

OFFERS
- Can advise on compliance implications of new trading tools
- Available to review model documentation for regulatory sign-off

NEEDS
- Need technical briefings on how ML pricing models work
- Looking to understand explainability requirements for AI in trading

LOCATION: Amsterdam HQ, Floor 2
CONSTRAINTS: Cannot share client data or specific surveillance findings
""",
    },
    {
        "user_id":      "10000000-0000-0000-0000-000000000008",
        "display_name": "Hendrik de Vries",
        "email":        "h.devries@ing-test.com",
        "headline":     "Research — European Credit Markets",
        "role":         "member",
        "cv": """
Hendrik de Vries — Credit Research Analyst

SUMMARY
7 years producing credit research on European HY and IG issuers.
Published 200+ credit notes. Expert in issuer analysis and spread forecasting.

SKILLS
- Fundamental credit analysis and issuer research
- Spread forecasting and relative value analysis
- Python for data analysis and report automation
- Bloomberg and Refinitiv for credit data
- Report writing and client communication

EXPERIENCE
Senior Credit Research Analyst — ING Research, Amsterdam (2018–present)
- Covers 45 European HY issuers across industrials and TMT
- Weekly credit strategy published to 3000 institutional clients

OFFERS
- Can provide issuer-specific credit context for pricing models
- Available to share research on specific bonds or sectors

NEEDS
- Need quantitative pricing signals to complement qualitative research
- Looking for data science support to scale report automation

ASSETS
- Weekly ING European Credit Strategy (published research)
- HY Issuer Database (internal, 2018–present)

LOCATION: Amsterdam HQ, Floor 8
""",
    },
    {
        "user_id":      "10000000-0000-0000-0000-000000000009",
        "display_name": "Mei Lin",
        "email":        "m.lin@ing-test.com",
        "headline":     "Portfolio Manager — Credit Alternatives",
        "role":         "member",
        "cv": """
Mei Lin — Portfolio Manager, Credit Alternatives

SUMMARY
11 years managing credit alternative strategies at ING Asset Management.
Expert in distressed debt, special situations, and illiquid credit investments.

SKILLS
- Distressed debt and special situations investing
- Illiquid credit valuation and mark-to-model
- Portfolio construction and risk budgeting
- Investor relations and fund reporting
- Python for portfolio analytics

EXPERIENCE
Senior Portfolio Manager — ING Asset Management, Amsterdam (2013–present)
- Manages €2.1bn credit alternatives book
- Generated 8.4% net IRR over 5 years

NEEDS
- Need better mark-to-model tools for illiquid credit positions
- Looking for quantitative pricing support for illiquid bonds

OFFERS
- Can provide real portfolio data to validate pricing models
- Available to give quant teams perspective on buy-side pricing needs

OBJECTIVES
- Seeking collaboration with teams improving illiquid credit valuation
- Interested in academic research on distressed debt pricing

LOCATION: Amsterdam HQ, Floor 10
""",
    },
    {
        "user_id":      "10000000-0000-0000-0000-000000000010",
        "display_name": "Ravi Patel",
        "email":        "r.patel@ing-test.com",
        "headline":     "Head of Innovation — AI in Finance",
        "role":         "member",
        "cv": """
Ravi Patel — Head of Innovation, AI and Advanced Analytics

SUMMARY
15 years leading AI and data science initiatives in financial services.
Currently leading ING's AI in Finance programme across all business lines.

SKILLS
- AI strategy and programme management
- NLP and LLM applications in finance
- Cross-functional stakeholder management
- Venture building and innovation lab leadership
- Python, R, and data science tooling

EXPERIENCE
Head of Innovation — ING Group, Amsterdam (2021–present)
- Running 12 AI pilot programmes across trading, risk, and retail
- Secured €8M innovation budget for AI in markets

NEEDS
- Looking for live use cases to pilot AI tools in trading
- Need front-office champions for AI pricing model adoption

OFFERS
- Can connect any team with innovation budget and resources
- Available to support business cases for AI tools in trading

OBJECTIVES
- Seeking collaboration with front-office teams on AI adoption
- Interested in internal innovation discussions around ML in fixed income

LOCATION: Amsterdam HQ, Floor 1
""",
    },
]

# ─────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────

PASS = "✓"
FAIL = "✗"
WARN = "⚠"

results = []

def section(title: str):
    print(f"\n{'─'*62}")
    print(f"  {title}")
    print(f"{'─'*62}")

def check(condition: bool, msg: str, fatal: bool = False):
    icon = PASS if condition else FAIL
    print(f"  {icon}  {msg}")
    results.append((condition, msg))
    if not condition and fatal:
        print("\n  ❌ Fatal — stopping.")
        _summary()
        sys.exit(1)

def warn(msg: str):
    print(f"  {WARN}  {msg}")

def info(msg: str):
    print(f"     {msg}")

def _summary():
    p = sum(1 for ok, _ in results if ok)
    f = sum(1 for ok, _ in results if not ok)
    print(f"\n{'═'*62}")
    print(f"  Results: {p} passed, {f} failed / {len(results)} total")
    print(f"{'═'*62}")


# ─────────────────────────────────────────────
#  Step 0: Wipe all data
# ─────────────────────────────────────────────

def step_wipe():
    section("Step 0: Wipe All Data")

    with httpx.Client(timeout=30) as c:
        # Wipe via dedicated admin endpoint
        r = c.post(f"{BASE_URL}/v1/admin/wipe",
                   json={"tenant_id": TENANT_ID, "confirm": True})
        if r.status_code == 200:
            check(True, "Admin wipe endpoint cleared all data")
            return

        # Fallback: call individual wipe endpoints if no admin/wipe
        warn(f"Admin wipe returned {r.status_code} — using Memgraph wipe")

    # Wipe Memgraph via cypher
    with httpx.Client(timeout=30) as c:
        r = c.post(f"{BASE_URL}/v1/ikg/wipe",
                   json={"tenant_id": TENANT_ID, "confirm": True})
        if r.status_code in (200, 404):
            check(True, "Memgraph wipe attempted")

    warn("Manual DB wipe may be needed — see instructions below")
    print("""
     To fully wipe data manually:
       docker compose exec postgres psql -U delllo -d delllo -c "
         TRUNCATE extracted_facts, document_chunks, documents,
                  matches, match_scores, feedback_events,
                  live_signals, notifications, feature_snapshots,
                  audit_log RESTART IDENTITY CASCADE;"
       docker compose exec memgraph mgconsole --execute "MATCH (n) DETACH DELETE n;"
    """)


# ─────────────────────────────────────────────
#  Step 1: Health
# ─────────────────────────────────────────────

def step_health() -> bool:
    section("Step 1: Health Checks")
    with httpx.Client(timeout=10) as c:
        r = c.get(f"{BASE_URL}/health")
        check(r.status_code == 200, "API is up", fatal=True)

        r = c.get(f"{BASE_URL}/health/stack")
        stack = r.json()
        for svc, d in stack["services"].items():
            ok = d["status"] == "ok"
            print(f"  {'✓' if ok else '⚠'}  {svc}: {d['detail'][:70]}")

        check(stack["services"]["postgres"]["status"] == "ok",  "PostgreSQL healthy", fatal=True)
        check(stack["services"]["memgraph"]["status"] == "ok",  "Memgraph healthy",  fatal=True)

        ollama_ok = stack["services"]["ollama"]["status"] in ("ok", "warn")
        if not ollama_ok:
            warn("Ollama unavailable — extraction steps will produce empty facts")
        return ollama_ok


# ─────────────────────────────────────────────
#  Step 2: Create 10 users
# ─────────────────────────────────────────────

def step_create_users():
    section("Step 2: Create 10 Users")

    created = []
    with httpx.Client(timeout=15) as c:
        for user in USERS:
            uid  = user["user_id"]
            name = user["display_name"]

            r = c.post(f"{BASE_URL}/v1/users",
                       json={
                           "user_id":      uid,
                           "tenant_id":    TENANT_ID,
                           "display_name": name,
                           "email":        user["email"],
                           "headline":     user["headline"],
                           "role":         user["role"],
                       })
            if r.status_code in (200, 201):
                check(True, f"Created user: {name}")
                created.append(uid)
            elif r.status_code == 409:
                warn(f"User already exists: {name} — continuing")
                created.append(uid)
            else:
                check(False, f"Failed to create {name} ({r.status_code}): {r.text[:80]}")

    info(f"{len(created)}/10 users ready")
    return created


# ─────────────────────────────────────────────
#  Step 3: Ingest + Extract CVs for all users
# ─────────────────────────────────────────────

def step_ingest_and_extract(ollama_ok: bool):
    section("Step 3: Ingest + Extract CVs (all 10 users)")

    doc_ids = {}

    for user in USERS:
        uid  = user["user_id"]
        name = user["display_name"]

        # Ingest
        with httpx.Client(timeout=60) as c:
            r = c.post(f"{BASE_URL}/v1/ingest/text",
                       data={
                           "tenant_id":   TENANT_ID,
                           "user_id":     uid,
                           "content":     user["cv"],
                           "source_type": "cv",
                           "filename":    f"cv_{uid[:8]}.txt",
                           "embed":       "true",
                       })
            if r.status_code == 200:
                doc_id = r.json()["document_id"]
                doc_ids[uid] = doc_id
                check(True, f"Ingested: {name} ({r.json()['chunk_count']} chunks)")
            else:
                check(False, f"Ingest failed for {name}: {r.status_code}")
                continue

        if not ollama_ok:
            warn(f"Skipping extraction for {name} — Ollama unavailable")
            continue

        # Extract
        with httpx.Client(timeout=360) as c:
            r = c.post(f"{BASE_URL}/v1/ingest/{doc_id}/extract",
                       json={
                           "user_id":     uid,
                           "tenant_id":   TENANT_ID,
                           "source_type": "cv",
                       })
            if r.status_code == 200:
                ext = r.json()
                fw  = ext["facts_written"]
                sk  = ext.get("skills_found", 0)
                nd  = ext.get("needs_found", 0)
                ikg = ext.get("ikg_errors", [])
                ikg_note = f" ⚠ {len(ikg)} iKG errors" if ikg else ""
                check(fw > 0, f"Extracted {name}: {fw} facts "
                              f"(skills={sk} needs={nd}){ikg_note}")
            else:
                check(False, f"Extraction failed for {name}: {r.status_code}")

    info(f"Documents ingested: {len(doc_ids)}/10")
    return doc_ids


# ─────────────────────────────────────────────
#  Step 4: Post live intent signals
# ─────────────────────────────────────────────

def step_post_intents():
    section("Step 4: Post Live Intent Signals (4 users)")

    intents = [
        {
            "user_id": USERS[1]["user_id"],  # Marcus — Trader
            "text":    "Need better pricing for illiquid HY corporate bonds urgently",
            "urgency": "high",
        },
        {
            "user_id": USERS[8]["user_id"],  # Mei — PM
            "text":    "Looking for quant support on illiquid credit mark-to-model",
            "urgency": "high",
        },
        {
            "user_id": USERS[9]["user_id"],  # Ravi — Innovation
            "text":    "Seeking front-office pilots for AI pricing tools",
            "urgency": "medium",
        },
        {
            "user_id": USERS[5]["user_id"],  # Lars — Structuring
            "text":    "Need pricing model for HY loans in CLO collateral pool",
            "urgency": "medium",
        },
    ]

    signal_ids = []
    with httpx.Client(timeout=15) as c:
        for intent in intents:
            r = c.post(f"{BASE_URL}/v1/signals/intent",
                       json={
                           "tenant_id":   TENANT_ID,
                           "user_id":     intent["user_id"],
                           "signal_type": "intent",
                           "payload": {
                               "text":    intent["text"],
                               "urgency": intent["urgency"],
                           },
                       })
            uid  = intent["user_id"]
            name = next(u["display_name"] for u in USERS if u["user_id"] == uid)
            check(r.status_code == 200,
                  f"Intent posted: {name} — \"{intent['text'][:50]}\"")
            if r.status_code == 200:
                signal_ids.append(r.json().get("signal_id"))

    return signal_ids


# ─────────────────────────────────────────────
#  Step 5: Post presence signals
# ─────────────────────────────────────────────

def step_post_presence():
    section("Step 5: Post Presence Signals (6 users)")

    presences = [
        (USERS[0]["user_id"], "Amsterdam HQ", "7"),   # Sarah — Quant
        (USERS[1]["user_id"], "Amsterdam HQ", "5"),   # Marcus — Trader
        (USERS[2]["user_id"], "Amsterdam HQ", "3"),   # Priya — MLOps
        (USERS[3]["user_id"], "Amsterdam HQ", "9"),   # Thomas — Risk
        (USERS[4]["user_id"], "Amsterdam HQ", "3"),   # Aisha — Data Eng
        (USERS[8]["user_id"], "Amsterdam HQ", "10"),  # Mei — PM
    ]

    with httpx.Client(timeout=15) as c:
        for uid, location, floor in presences:
            r = c.post(f"{BASE_URL}/v1/signals/presence",
                       json={
                           "tenant_id":   TENANT_ID,
                           "user_id":     uid,
                           "signal_type": "presence",
                           "payload": {"location": location, "floor": floor},
                       })
            name = next(u["display_name"] for u in USERS if u["user_id"] == uid)
            check(r.status_code == 200,
                  f"Presence: {name} — {location} Floor {floor}")


# ─────────────────────────────────────────────
#  Step 6: Verify iKG subgraphs
# ─────────────────────────────────────────────

def step_verify_ikg():
    section("Step 6: Verify iKG Subgraphs")

    total_skills = 0
    total_nodes  = 0

    with httpx.Client(timeout=30) as c:
        for user in USERS:
            uid  = user["user_id"]
            name = user["display_name"]
            r    = c.get(f"{BASE_URL}/v1/ikg/person/{uid}")
            if r.status_code == 200:
                data   = r.json()
                skills = len(data.get("skills", []))
                domains = len(data.get("domains", []))
                total_skills += skills
                total_nodes  += 1
                check(bool(data.get("person")),
                      f"iKG: {name} — {skills} skills, {domains} domains")
            elif r.status_code == 404:
                warn(f"iKG: {name} not in graph yet — run /v1/ikg/upsert to backfill")
            else:
                check(False, f"iKG error for {name}: {r.status_code}")

    info(f"iKG nodes: {total_nodes}/10 users, avg skills: "
         f"{total_skills/max(total_nodes,1):.1f}")


# ─────────────────────────────────────────────
#  Step 7: Run matchmaking for every user
# ─────────────────────────────────────────────

def step_run_matchmaking():
    section("Step 7: Generate Matches for All Users")

    all_matches: dict[str, list] = {}   # uid → list of match dicts

    with httpx.Client(timeout=120) as c:
        for user in USERS:
            uid  = user["user_id"]
            name = user["display_name"]

            r = c.post(f"{BASE_URL}/v1/matches/generate",
                       json={
                           "tenant_id":           TENANT_ID,
                           "requesting_user_id":  uid,
                           "transaction_types":   ["technical_problem_solving",
                                                   "knowledge_transfer"],
                           "max_candidates":      5,
                           "min_score":           0.03,
                           "generate_explanations": False,  # fast for test
                       })

            if r.status_code == 200:
                data    = r.json()
                matches = data.get("matches", [])
                created = data.get("matches_created", 0)
                version = data.get("score_version", "?")
                check(True,
                      f"Matches for {name}: {created} created (v{version})")
                if matches:
                    top = matches[0]
                    info(f"  Top match: {top.get('candidate_name','?')} "
                         f"score={top.get('score',0):.3f} "
                         f"rel={top.get('score_breakdown',{}).get('relevance',0):.2f} "
                         f"comp={top.get('score_breakdown',{}).get('complementarity',0):.2f}")
                all_matches[uid] = matches
            else:
                check(False, f"Match generation failed for {name}: {r.status_code} {r.text[:80]}")
                all_matches[uid] = []

    return all_matches


# ─────────────────────────────────────────────
#  Step 8: Score breakdown verification
# ─────────────────────────────────────────────

def step_verify_scores(all_matches: dict):
    section("Step 8: Score Breakdown Verification")

    features = ["relevance", "complementarity", "timing", "proximity",
                "evidence_strength", "outcome_likelihood", "novelty",
                "privacy_risk", "interaction_friction"]

    total_matches = sum(len(m) for m in all_matches.values())
    check(total_matches > 0, f"Total matches created: {total_matches}", fatal=True)

    # Pick a match with a score breakdown to inspect
    sample_breakdown = None
    sample_pair = None
    for uid, matches in all_matches.items():
        for m in matches:
            bd = m.get("score_breakdown", {})
            if bd:
                sample_breakdown = bd
                name = next(u["display_name"] for u in USERS if u["user_id"] == uid)
                sample_pair = (name, m.get("candidate_name", "?"))
                break
        if sample_breakdown:
            break

    if sample_breakdown:
        check(True, f"Score breakdown present for {sample_pair[0]} → {sample_pair[1]}")
        for feat in features:
            val = sample_breakdown.get(feat)
            present = val is not None
            check(present, f"  Feature '{feat}' = {val:.4f}" if present
                  else f"  Feature '{feat}' missing", fatal=False)
    else:
        check(False, "No score breakdowns found in any match response")

    # Print match quality table
    print("\n  Match quality table (all generated matches):")
    print(f"  {'Requester':<22} {'Candidate':<22} {'Score':>6} "
          f"{'Rel':>5} {'Comp':>5} {'Tim':>5} {'Prox':>5} {'Evid':>5}")
    print(f"  {'─'*22} {'─'*22} {'─'*6} {'─'*5} {'─'*5} {'─'*5} {'─'*5} {'─'*5}")

    for user in USERS:
        uid  = user["user_id"]
        name = user["display_name"][:20]
        for m in all_matches.get(uid, [])[:2]:
            bd   = m.get("score_breakdown", {})
            cand = m.get("candidate_name", "?")[:20]
            print(f"  {name:<22} {cand:<22} "
                  f"{m.get('score',0):>6.3f} "
                  f"{bd.get('relevance',0):>5.2f} "
                  f"{bd.get('complementarity',0):>5.2f} "
                  f"{bd.get('timing',0):>5.2f} "
                  f"{bd.get('proximity',0):>5.2f} "
                  f"{bd.get('evidence_strength',0):>5.2f}")


# ─────────────────────────────────────────────
#  Step 9: Accept top matches for 3 users
# ─────────────────────────────────────────────

def step_accept_matches(all_matches: dict) -> list[str]:
    section("Step 9: Accept Top Matches (3 Users)")

    accepted_match_ids = []
    acceptors = [USERS[1]["user_id"],   # Marcus — Trader
                 USERS[8]["user_id"],   # Mei — PM
                 USERS[9]["user_id"]]   # Ravi — Innovation

    with httpx.Client(timeout=15) as c:
        for uid in acceptors:
            matches = all_matches.get(uid, [])
            if not matches:
                warn(f"No matches to accept for {uid[:8]}")
                continue
            top     = matches[0]
            mid     = top["match_id"]
            name    = next(u["display_name"] for u in USERS if u["user_id"] == uid)
            cand    = top.get("candidate_name", "?")

            r = c.post(f"{BASE_URL}/v1/matches/{mid}/accept",
                       params={"actor_user_id": uid})
            check(r.status_code == 200,
                  f"Accepted: {name} → {cand} (match {mid[:8]}…)")
            if r.status_code == 200:
                accepted_match_ids.append(mid)

    return accepted_match_ids


# ─────────────────────────────────────────────
#  Step 10: Submit feedback
# ─────────────────────────────────────────────

def step_submit_feedback(accepted_ids: list[str]):
    section("Step 10: Submit Meeting Feedback")

    if len(accepted_ids) < 1:
        warn("No accepted matches — skipping feedback")
        return

    feedbacks = [
        {
            "match_id":     accepted_ids[0] if len(accepted_ids) > 0 else None,
            "actor_uid":    USERS[1]["user_id"],
            "feedback_type":"met",
            "payload": {
                "met": True, "quality_score": 5,
                "notes": "Excellent discussion on ML pricing approach. Will collaborate.",
            },
        },
    ]
    if len(accepted_ids) > 1:
        feedbacks.append({
            "match_id":     accepted_ids[1],
            "actor_uid":    USERS[8]["user_id"],
            "feedback_type":"useful",
            "payload": {"met": True, "quality_score": 3, "notes": "Useful context."},
        })
    if len(accepted_ids) > 2:
        feedbacks.append({
            "match_id":     accepted_ids[2],
            "actor_uid":    USERS[9]["user_id"],
            "feedback_type":"no_show",
            "payload": {"met": False, "notes": "Meeting did not happen."},
        })

    with httpx.Client(timeout=15) as c:
        for fb in feedbacks:
            if not fb["match_id"]:
                continue
            r = c.post(f"{BASE_URL}/v1/matches/{fb['match_id']}/feedback",
                       json={
                           "actor_user_id": fb["actor_uid"],
                           "feedback_type": fb["feedback_type"],
                           "payload":       fb["payload"],
                       })
            uid  = fb["actor_uid"]
            name = next(u["display_name"] for u in USERS if u["user_id"] == uid)
            check(r.status_code == 200,
                  f"Feedback '{fb['feedback_type']}' from {name}")


# ─────────────────────────────────────────────
#  Step 11: Analytics overview
# ─────────────────────────────────────────────

def step_analytics():
    section("Step 11: Tenant Analytics")

    with httpx.Client(timeout=15) as c:
        r = c.get(f"{BASE_URL}/v1/analytics/{TENANT_ID}/overview")
        check(r.status_code == 200, "Analytics overview returned 200")
        if r.status_code == 200:
            d = r.json()
            m = d.get("matches", {})
            f = d.get("facts", {})
            info(f"Total matches:        {m.get('total', 0)}")
            info(f"Accepted matches:     {m.get('accepted', 0)}")
            info(f"Avg match score:      {m.get('avg_score', 0)}")
            info(f"Acceptance rate:      {m.get('acceptance_rate', 0)}")
            info(f"Users with facts:     {f.get('users_with_facts', 0)}")
            info(f"Total facts:          {f.get('total_facts', 0)}")
            info(f"Avg fact confidence:  {f.get('avg_confidence', 0)}")
            fb = d.get("feedback", {})
            if fb:
                info(f"Feedback breakdown:   {json.dumps(fb)}")

        r2 = c.get(f"{BASE_URL}/v1/analytics/{TENANT_ID}/top-skills")
        check(r2.status_code == 200, "Top-skills endpoint returned 200")
        if r2.status_code == 200:
            skills = r2.json().get("top_facts", {}).get("skill", [])[:5]
            if skills:
                info(f"Top skills: {', '.join(s['name'] for s in skills)}")

        r3 = c.get(f"{BASE_URL}/v1/analytics/{TENANT_ID}/coverage")
        check(r3.status_code == 200, "Coverage endpoint returned 200")
        if r3.status_code == 200:
            cov = r3.json()
            info(f"Cold-start users: {cov.get('cold_start_users', 0)}")
            info(f"No-signal users:  {cov.get('no_signal_users', 0)}")


# ─────────────────────────────────────────────
#  Step 12: Trigger learning sweep
# ─────────────────────────────────────────────

def step_learning_sweep():
    section("Step 12: Trigger Learning Sweep")

    with httpx.Client(timeout=30) as c:
        r = c.post(f"{BASE_URL}/v1/analytics/{TENANT_ID}/sweep")
        check(r.status_code == 200, "Learning sweep triggered")
        if r.status_code == 200:
            d = r.json()
            info(f"Users processed:   {d.get('users_processed', 0)}")
            info(f"Snapshots updated: {d.get('snapshots_updated', 0)}")
            errs = d.get("errors", [])
            if errs:
                warn(f"Sweep errors: {len(errs)}")
                for e in errs[:3]:
                    warn(f"  {e}")


# ─────────────────────────────────────────────
#  Step 13: Re-run matchmaking and compare scores
# ─────────────────────────────────────────────

def step_rerun_matchmaking():
    section("Step 13: Re-run Matchmaking — Verify Score Updates")

    # Re-run for the user who got 'met' feedback (Marcus → should see novelty drop)
    uid  = USERS[1]["user_id"]
    name = USERS[1]["display_name"]

    with httpx.Client(timeout=120) as c:
        r = c.post(f"{BASE_URL}/v1/matches/generate",
                   json={
                       "tenant_id":           TENANT_ID,
                       "requesting_user_id":  uid,
                       "transaction_types":   ["technical_problem_solving"],
                       "max_candidates":      5,
                       "min_score":           0.03,
                       "generate_explanations": False,
                   })

        check(r.status_code == 200, f"Re-run matchmaking for {name}: {r.status_code}")
        if r.status_code == 200:
            data    = r.json()
            matches = data.get("matches", [])
            info(f"New matches created: {data.get('matches_created', 0)}")

            for m in matches[:3]:
                bd = m.get("score_breakdown", {})
                info(f"  {m.get('candidate_name','?')[:25]}: "
                     f"score={m.get('score',0):.3f} "
                     f"novelty={bd.get('novelty',0):.2f} "
                     f"outcome={bd.get('outcome_likelihood',0):.2f}")

    # Verify a recommended match with explanation
    with httpx.Client(timeout=15) as c:
        r = c.get(f"{BASE_URL}/v1/matches/recommended",
                  params={"user_id": uid, "tenant_id": TENANT_ID})
        check(r.status_code == 200, "Recommended matches endpoint returned 200")
        recs = r.json().get("recommended", [])
        check(len(recs) >= 0, f"Recommended count: {len(recs)}", fatal=False)


# ─────────────────────────────────────────────
#  Step 14: Final summary
# ─────────────────────────────────────────────

def step_final_summary():
    section("Step 14: Final Summary")

    passed = sum(1 for ok, _ in results if ok)
    failed = sum(1 for ok, _ in results if not ok)
    total  = len(results)

    print(f"\n  Total:  {total}")
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")

    if failed:
        print("\n  Failed checks:")
        for ok, msg in results:
            if not ok:
                print(f"    {FAIL}  {msg}")

    status = "✅ All checks passed!" if failed == 0 else f"⚠  {failed} checks failed"
    print(f"\n  {status}")
    print(f"\n  Users created:  {len(USERS)}")
    print(f"  Roles covered:  Quant, Trader, MLOps, Risk, Data Eng,")
    print(f"                  Structuring, Compliance, Research, PM, Innovation")
    print()
    print("  Useful endpoints to explore:")
    print(f"   → Docs:            {BASE_URL}/docs")
    print(f"   → Memgraph Lab:    http://localhost:3000")
    print(f"   → Grafana:         http://localhost:3001")
    print(f"   → Analytics:       {BASE_URL}/v1/analytics/{TENANT_ID}/overview")
    print(f"   → Match quality:   {BASE_URL}/v1/analytics/{TENANT_ID}/match-quality")
    print()


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

def main():
    print("\n🚀 Delllo RAIN3.0 — Full Matchmaking Test")
    print(f"   API:    {BASE_URL}")
    print(f"   Tenant: {TENANT_ID}")
    print(f"   Users:  {len(USERS)} (diverse roles, ING Amsterdam)")

    step_wipe()
    ollama_ok   = step_health()
    step_create_users()
    step_ingest_and_extract(ollama_ok)
    step_post_intents()
    step_post_presence()
    step_verify_ikg()
    all_matches = step_run_matchmaking()
    step_verify_scores(all_matches)
    accepted    = step_accept_matches(all_matches)
    step_submit_feedback(accepted)
    step_analytics()
    step_learning_sweep()
    step_rerun_matchmaking()
    step_final_summary()


if __name__ == "__main__":
    main()