"""
Delllo RAIN3.0 — Memgraph Graph Schema Init
Run this ONCE after Memgraph is up to create
constraints, indexes, and seed the gKG ontology.

Usage:
    python scripts/init_graph.py
"""

import os
import sys
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

MEMGRAPH_URI      = f"bolt://{os.getenv('MEMGRAPH_HOST', 'localhost')}:{os.getenv('MEMGRAPH_PORT', '7687')}"
MEMGRAPH_USER     = os.getenv("MEMGRAPH_USER", "memgraph")
MEMGRAPH_PASSWORD = os.getenv("MEMGRAPH_PASSWORD", "memgraph_secret")


# ─────────────────────────────────────────────
#  SCHEMA: Constraints + Indexes
#  Memgraph uses Cypher — same as Neo4j
# ─────────────────────────────────────────────

CONSTRAINTS = [
    # iKG nodes
    "CREATE CONSTRAINT ON (p:Person)          ASSERT p.person_id       IS UNIQUE",
    "CREATE CONSTRAINT ON (s:Skill)           ASSERT s.skill_id        IS UNIQUE",
    "CREATE CONSTRAINT ON (d:Domain)          ASSERT d.domain_id       IS UNIQUE",
    "CREATE CONSTRAINT ON (o:Objective)       ASSERT o.objective_id    IS UNIQUE",
    "CREATE CONSTRAINT ON (of:Offer)          ASSERT of.offer_id       IS UNIQUE",
    "CREATE CONSTRAINT ON (a:Achievement)     ASSERT a.achievement_id  IS UNIQUE",
    "CREATE CONSTRAINT ON (ev:Evidence)       ASSERT ev.evidence_id    IS UNIQUE",
    "CREATE CONSTRAINT ON (pr:Project)        ASSERT pr.project_id     IS UNIQUE",

    # gKG nodes
    "CREATE CONSTRAINT ON (tt:TransactionType) ASSERT tt.type_id       IS UNIQUE",
    "CREATE CONSTRAINT ON (pt:ProblemType)     ASSERT pt.type_id       IS UNIQUE",
    "CREATE CONSTRAINT ON (ct:CapabilityType)  ASSERT ct.type_id       IS UNIQUE",
    "CREATE CONSTRAINT ON (ctx:ContextType)    ASSERT ctx.type_id      IS UNIQUE",

    # sKG nodes
    "CREATE CONSTRAINT ON (li:LiveIntent)      ASSERT li.intent_id     IS UNIQUE",
    "CREATE CONSTRAINT ON (pr:Presence)        ASSERT pr.presence_id   IS UNIQUE",

    # oKG nodes
    "CREATE CONSTRAINT ON (mr:MatchRecommendation) ASSERT mr.match_id  IS UNIQUE",
    "CREATE CONSTRAINT ON (io:InteractionOutcome)  ASSERT io.outcome_id IS UNIQUE",
]

INDEXES = [
    # Person lookups by tenant
    "CREATE INDEX ON :Person(tenant_id)",
    "CREATE INDEX ON :Person(display_name)",

    # Skill + domain name search
    "CREATE INDEX ON :Skill(canonical_name)",
    "CREATE INDEX ON :Domain(canonical_name)",

    # Evidence freshness
    "CREATE INDEX ON :Evidence(freshness_date)",
    "CREATE INDEX ON :Evidence(confidence)",

    # Objective validity
    "CREATE INDEX ON :Objective(status)",
    "CREATE INDEX ON :Objective(urgency)",

    # Signal freshness
    "CREATE INDEX ON :LiveIntent(created_at)",
    "CREATE INDEX ON :Presence(last_seen)",
]


# ─────────────────────────────────────────────
#  gKG SEED DATA — Transaction Types + Logic
# ─────────────────────────────────────────────

GKG_TRANSACTION_TYPES = [
    {
        "type_id": "tt_knowledge_transfer",
        "name": "Knowledge Transfer",
        "description": "One party has deep expertise the other needs. Asymmetric value exchange.",
        "sector": "general",
    },
    {
        "type_id": "tt_technical_problem_solving",
        "name": "Technical Problem Solving",
        "description": "Requester has a concrete technical problem. Match brings a solution capability.",
        "sector": "general",
    },
    {
        "type_id": "tt_product_ideation",
        "name": "Product Ideation",
        "description": "Collaborative exploration of new product directions. Both parties contribute ideas.",
        "sector": "general",
    },
    {
        "type_id": "tt_sales_introduction",
        "name": "Sales Introduction",
        "description": "Connecting a seller with a qualified buyer or decision-maker.",
        "sector": "general",
    },
    {
        "type_id": "tt_hiring",
        "name": "Hiring",
        "description": "Matching talent to an open role or team need.",
        "sector": "general",
    },
    {
        "type_id": "tt_investor_matching",
        "name": "Investor Matching",
        "description": "Connecting founders or projects with investors aligned to the domain.",
        "sector": "general",
    },
    {
        "type_id": "tt_internal_innovation",
        "name": "Internal Innovation",
        "description": "Cross-team collaboration within an enterprise to drive new initiatives.",
        "sector": "enterprise",
    },
    {
        "type_id": "tt_regulatory_guidance",
        "name": "Regulatory Guidance",
        "description": "Expert in compliance/regulation connects with team navigating a regulatory question.",
        "sector": "financial_services",
    },
    {
        "type_id": "tt_partnership_formation",
        "name": "Partnership Formation",
        "description": "Two parties exploring a strategic alliance or joint initiative.",
        "sector": "general",
    },
]

GKG_PROBLEM_TYPES = [
    {
        "type_id": "pt_illiquid_bond_pricing",
        "name": "Illiquid Bond Pricing",
        "description": "Pricing corporate bonds where observable market trades are sparse",
        "sector": "financial_services",
    },
    {
        "type_id": "pt_credit_risk_modelling",
        "name": "Credit Risk Modelling",
        "description": "Building or improving statistical models for credit default/spread risk",
        "sector": "financial_services",
    },
    {
        "type_id": "pt_ml_model_deployment",
        "name": "ML Model Deployment",
        "description": "Taking a trained model from research to production at scale",
        "sector": "technology",
    },
]

GKG_CAPABILITY_TYPES = [
    {"type_id": "cap_credit_model_design",         "name": "Credit Model Design"},
    {"type_id": "cap_market_microstructure",        "name": "Market Microstructure Knowledge"},
    {"type_id": "cap_ml_pricing",                   "name": "ML-Based Pricing"},
    {"type_id": "cap_fixed_income_domain",          "name": "Fixed Income Domain Expertise"},
    {"type_id": "cap_python_quant",                 "name": "Python Quantitative Development"},
    {"type_id": "cap_regulatory_compliance",        "name": "Regulatory Compliance"},
]

GKG_CONTEXT_TYPES = [
    {"type_id": "ctx_same_office",      "name": "Same Office",      "boost_weight": 0.15},
    {"type_id": "ctx_same_floor",       "name": "Same Floor",       "boost_weight": 0.10},
    {"type_id": "ctx_high_urgency",     "name": "High Urgency",     "boost_weight": 0.20},
    {"type_id": "ctx_same_event",       "name": "Same Event",       "boost_weight": 0.25},
    {"type_id": "ctx_active_session",   "name": "Active Session",   "boost_weight": 0.18},
]

# Problem → Capability REQUIRES relationships
GKG_REQUIRES_EDGES = [
    ("pt_illiquid_bond_pricing",  "cap_credit_model_design",    0.90),
    ("pt_illiquid_bond_pricing",  "cap_market_microstructure",  0.75),
    ("pt_illiquid_bond_pricing",  "cap_ml_pricing",             0.80),
    ("pt_illiquid_bond_pricing",  "cap_fixed_income_domain",    0.85),
    ("pt_credit_risk_modelling",  "cap_credit_model_design",    0.95),
    ("pt_credit_risk_modelling",  "cap_python_quant",           0.70),
    ("pt_ml_model_deployment",    "cap_python_quant",           0.90),
]


# ─────────────────────────────────────────────
#  RUNNER
# ─────────────────────────────────────────────

def run_schema(driver):
    with driver.session() as session:
        print("Creating constraints...")
        for stmt in CONSTRAINTS:
            try:
                session.run(stmt)
                print(f"  ✓ {stmt[:60]}...")
            except Exception as e:
                # Constraint may already exist on re-run
                print(f"  ⚠ (already exists or error): {e}")

        print("\nCreating indexes...")
        for stmt in INDEXES:
            try:
                session.run(stmt)
                print(f"  ✓ {stmt[:60]}...")
            except Exception as e:
                print(f"  ⚠ {e}")


def seed_gkg(driver):
    with driver.session() as session:
        print("\nSeeding gKG — TransactionTypes...")
        for tt in GKG_TRANSACTION_TYPES:
            session.run(
                """
                MERGE (t:TransactionType {type_id: $type_id})
                SET t.name        = $name,
                    t.description = $description,
                    t.sector      = $sector,
                    t.active      = true
                """,
                **tt,
            )
        print(f"  ✓ {len(GKG_TRANSACTION_TYPES)} transaction types")

        print("Seeding gKG — ProblemTypes...")
        for pt in GKG_PROBLEM_TYPES:
            session.run(
                """
                MERGE (p:ProblemType {type_id: $type_id})
                SET p.name        = $name,
                    p.description = $description,
                    p.sector      = $sector
                """,
                **pt,
            )

        print("Seeding gKG — CapabilityTypes...")
        for ct in GKG_CAPABILITY_TYPES:
            session.run(
                """
                MERGE (c:CapabilityType {type_id: $type_id})
                SET c.name = $name
                """,
                **ct,
            )

        print("Seeding gKG — ContextTypes...")
        for ctx in GKG_CONTEXT_TYPES:
            session.run(
                """
                MERGE (c:ContextType {type_id: $type_id})
                SET c.name         = $name,
                    c.boost_weight = $boost_weight
                """,
                **ctx,
            )

        print("Seeding gKG — REQUIRES edges...")
        for problem_id, cap_id, weight in GKG_REQUIRES_EDGES:
            session.run(
                """
                MATCH (p:ProblemType     {type_id: $problem_id})
                MATCH (c:CapabilityType  {type_id: $cap_id})
                MERGE (p)-[r:REQUIRES]->(c)
                SET r.weight = $weight
                """,
                problem_id=problem_id,
                cap_id=cap_id,
                weight=weight,
            )
        print(f"  ✓ {len(GKG_REQUIRES_EDGES)} REQUIRES edges")

        # Wire financial services problem to transaction type
        session.run(
            """
            MATCH (tt:TransactionType {type_id: 'tt_technical_problem_solving'})
            MATCH (pt:ProblemType     {type_id: 'pt_illiquid_bond_pricing'})
            MERGE (pt)-[:MAPS_TO]->(tt)
            """
        )

        # Wire context boosts to transaction types
        for ctx_id in ["ctx_same_office", "ctx_high_urgency", "ctx_active_session"]:
            session.run(
                """
                MATCH (ctx:ContextType    {type_id: $ctx_id})
                MATCH (tt:TransactionType {type_id: 'tt_technical_problem_solving'})
                MERGE (ctx)-[:BOOSTS]->(tt)
                """,
                ctx_id=ctx_id,
            )

        print("  ✓ Context BOOSTS wired")


def seed_ikg_example(driver):
    """Seed the ING example from the design doc for testing."""
    with driver.session() as session:
        print("\nSeeding iKG example (ING Quant)...")

        # Person node
        session.run(
            """
            MERGE (p:Person {person_id: '00000000-0000-0000-0001-000000000003'})
            SET p.tenant_id    = '00000000-0000-0000-0000-000000000002',
                p.display_name = 'ING Quant',
                p.headline     = 'Quantitative Analyst — ML Pricing',
                p.seniority    = 'senior',
                p.department   = 'Quantitative Research',
                p.visibility   = 'match_engine_only'
            """
        )

        # Skill node
        session.run(
            """
            MERGE (s:Skill {skill_id: 'skill_ml_credit_pricing'})
            SET s.name           = 'ML Credit Pricing',
                s.canonical_name = 'ml_credit_pricing',
                s.category       = 'quantitative_finance'
            """
        )

        # HAS_SKILL edge with confidence
        session.run(
            """
            MATCH (p:Person {person_id: '00000000-0000-0000-0001-000000000003'})
            MATCH (s:Skill  {skill_id: 'skill_ml_credit_pricing'})
            MERGE (p)-[r:HAS_SKILL]->(s)
            SET r.confidence  = 0.91,
                r.visibility  = 'match_engine_only',
                r.validated   = true
            """
        )

        # Domain node
        session.run(
            """
            MERGE (d:Domain {domain_id: 'domain_corporate_bonds'})
            SET d.name           = 'Corporate Bonds',
                d.canonical_name = 'corporate_bonds',
                d.sector         = 'financial_services'
            """
        )

        session.run(
            """
            MATCH (p:Person {person_id: '00000000-0000-0000-0001-000000000003'})
            MATCH (d:Domain {domain_id: 'domain_corporate_bonds'})
            MERGE (p)-[:HAS_DOMAIN]->(d)
            """
        )

        print("  ✓ iKG example seeded")


def main():
    print(f"Connecting to Memgraph at {MEMGRAPH_URI}...")
    driver = GraphDatabase.driver(
        MEMGRAPH_URI,
        auth=(MEMGRAPH_USER, MEMGRAPH_PASSWORD)
    )

    try:
        driver.verify_connectivity()
        print("  ✓ Connected!\n")

        run_schema(driver)
        seed_gkg(driver)
        seed_ikg_example(driver)

        print("\n✅ Memgraph schema + seed data complete.")
        print("   Open Memgraph Lab at http://localhost:3000 to explore the graph.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("   Is Memgraph running? Try: docker compose up memgraph -d")
        raise
    finally:
        driver.close()


if __name__ == "__main__":
    main()
