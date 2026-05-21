"""
Delllo RAIN3.0 — Matchmaking End-to-End Test Script
=====================================================
Uses users_dataset.csv to:
  1. Create a tenant
  2. Upsert all 10 users
  3. Ingest + extract each user's bio (with embed=True)
  4. Post a live intent signal for user 1
  5. Trigger match generation for user 1
  6. Print a ranked match table
  7. Submit feedback on the top match

Usage:
    python scripts/test_matchmaking.py
    python scripts/test_matchmaking.py --base-url http://localhost:8000 --user 3
    python scripts/test_matchmaking.py --skip-ingest   # if data already loaded

Requires: httpx, rich  →  pip install httpx rich
"""

import argparse
import asyncio
import csv
import json
import sys
import time
from pathlib import Path

import httpx
from rich.console import Console
from rich.table import Table
from rich import print as rprint

# ─────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────

TENANT_ID   = "a1b2c3d4-0000-0000-0000-000000000001"
BASE_URL    = "http://localhost:8000"
CSV_PATH    = Path(__file__).parent / "users_dataset.csv"

# Fixed user UUIDs derived from dataset row id (deterministic, easy to repeat)
def user_uuid(row_id: int) -> str:
    return f"{row_id:08d}-0000-0000-0000-000000000001"

TRANSACTION_TYPES = [
    "technical_problem_solving",
    "knowledge_sharing",
    "collaboration",
    "mentorship",
    "hiring",
]

console = Console()


# ─────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────

def load_dataset() -> list[dict]:
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


async def post(client: httpx.AsyncClient, path: str, **kwargs) -> dict:
    resp = await client.post(f"{BASE_URL}/v1{path}", **kwargs)
    if resp.status_code not in (200, 201):
        console.print(f"[red]POST {path} → {resp.status_code}[/red]\n{resp.text[:300]}")
        return {}
    return resp.json()


async def get(client: httpx.AsyncClient, path: str, **kwargs) -> dict:
    resp = await client.get(f"{BASE_URL}/v1{path}", **kwargs)
    if resp.status_code not in (200, 201):
        console.print(f"[red]GET {path} → {resp.status_code}[/red]\n{resp.text[:300]}")
        return {}
    return resp.json()


# ─────────────────────────────────────────────
#  Step 1 — Create tenant
# ─────────────────────────────────────────────

async def ensure_tenant(client: httpx.AsyncClient):
    console.rule("[bold blue]Step 1 · Ensure Tenant")
    # Admin create_user auto-creates tenant on first call; just verify health
    resp = await client.get(f"{BASE_URL}/health")
    if resp.status_code == 200:
        console.print(f"[green]✓ API reachable[/green]  tenant_id={TENANT_ID[:8]}…")
    else:
        console.print(f"[red]✗ API not reachable at {BASE_URL}[/red]")
        sys.exit(1)


# ─────────────────────────────────────────────
#  Step 2 — Upsert all users
# ─────────────────────────────────────────────

async def upsert_users(client: httpx.AsyncClient, rows: list[dict]) -> list[dict]:
    console.rule("[bold blue]Step 2 · Upsert Users")
    users = []
    for row in rows:
        uid  = user_uuid(int(row["id"]))
        bio  = json.loads(row["bio_json"])
        data = {
            "user_id":      uid,
            "tenant_id":    TENANT_ID,
            "display_name": row["name"],
            "email":        row["email"],
            "headline":     bio.get("summary", "")[:120],
            "role":         "member",
            "status":       "active",
        }
        result = await post(client, "/users", json=data)
        if result:
            console.print(f"  [green]✓[/green] {row['name']:30s}  uid={uid[:8]}…")
            users.append({**row, "user_id": uid, "bio": bio})
        else:
            console.print(f"  [red]✗[/red] {row['name']}")
    console.print(f"\n[bold]Upserted {len(users)}/{len(rows)} users[/bold]")
    return users


# ─────────────────────────────────────────────
#  Step 3 — Ingest + Extract each user
# ─────────────────────────────────────────────

async def ingest_and_extract(client: httpx.AsyncClient, users: list[dict]):
    console.rule("[bold blue]Step 3 · Ingest + Extract (pipeline)")
    results = []
    for u in users:
        bio_text = json.dumps(u["bio"])
        # Use /ingest/pipeline: one call for ingest + extraction
        resp = await client.post(
            f"{BASE_URL}/v1/ingest/pipeline",
            data={
                "tenant_id":   TENANT_ID,
                "user_id":     u["user_id"],
                "source_type": "cv",
                "embed":       "true",
            },
            files={"file": (f"{u['name'].replace(' ','_')}_cv.txt",
                            bio_text.encode(), "text/plain")},
            timeout=30000.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            chunk_count = data.get("ingestion", {}).get("chunk_count", 0)
            facts       = data.get("extraction", {})
            fact_count  = getattr(facts, "facts_written", None) or \
                          (facts.get("facts_written") if isinstance(facts, dict) else "?")
            console.print(
                f"  [green]✓[/green] {u['name']:30s}  "
                f"chunks={chunk_count}  facts={fact_count}"
            )
            results.append({"user": u, "doc": data})
        else:
            console.print(f"  [red]✗[/red] {u['name']:30s}  {resp.status_code}: {resp.text[:120]}")
        await asyncio.sleep(0.2)   # be gentle with Ollama
    return results


# ─────────────────────────────────────────────
#  Step 4 — Post a live intent signal
# ─────────────────────────────────────────────

async def post_intent(client: httpx.AsyncClient, requester: dict):
    console.rule("[bold blue]Step 4 · Post Live Intent Signal")
    bio    = requester["bio"]
    skills = bio.get("skills", [])[:3]
    intent_text = (
        f"Looking to collaborate on distributed systems and cloud architecture. "
        f"I have expertise in {', '.join(skills)} and want to connect with people "
        f"working on similar challenges."
    )
    payload = {
        "tenant_id":   TENANT_ID,
        "user_id":     requester["user_id"],
        "signal_type": "intent",
        "payload": {
            "text":    intent_text,
            "urgency": "medium",
        },
    }
    result = await post(client, "/signals/intent", json=payload)
    if result:
        console.print(f"  [green]✓[/green] Intent posted for {requester['name']}")
        console.print(f"  [dim]{intent_text[:100]}…[/dim]")
    return result


# ─────────────────────────────────────────────
#  Step 5 — Generate matches
# ─────────────────────────────────────────────

async def generate_matches(
    client: httpx.AsyncClient,
    requester: dict,
    transaction_type: str,
) -> dict:
    console.rule(f"[bold blue]Step 5 · Generate Matches  [{transaction_type}]")
    payload = {
    "tenant_id":             TENANT_ID,
    "requesting_user_id":    requester["user_id"],
    "transaction_types":     [transaction_type],
    "max_candidates":        3,
    "min_score":             0.01,
    "generate_explanations": True,
    }
    start = time.perf_counter()
    result = await post(client, "/matches/generate", json=payload, timeout=600.0)
    elapsed = time.perf_counter() - start

    if not result:
        console.print("[red]Match generation returned empty response[/red]")
        return {}

    console.print(f"  [dim]Completed in {elapsed:.2f}s[/dim]")
    return result


# ─────────────────────────────────────────────
#  Step 6 — Print ranked match table
# ─────────────────────────────────────────────

def print_match_table(matches_response: dict, users: list[dict], requester: dict):
    console.rule("[bold blue]Step 6 · Match Results")

    uid_to_name = {u["user_id"]: u["name"] for u in users}
    matches = matches_response.get("matches", [])

    if not matches:
        console.print("[yellow]No matches returned.[/yellow]")
        console.print(f"[dim]Full response: {json.dumps(matches_response, indent=2)[:600]}[/dim]")
        return

    console.print(
        f"[bold]Requester:[/bold] {requester['name']} "
        f"(uid={requester['user_id'][:8]}…)\n"
        f"[bold]Transaction type:[/bold] {matches_response.get('transaction_type', '—')}\n"
    )

    table = Table(show_header=True, header_style="bold cyan", box=None, padding=(0,1))
    table.add_column("#",            width=3)
    table.add_column("Candidate",    width=22)
    table.add_column("Score",        width=7)
    table.add_column("Relevance",    width=10)
    table.add_column("Evidence",     width=10)
    table.add_column("Timing",       width=8)
    table.add_column("Explanation",  width=55)

    for i, m in enumerate(matches, 1):
        breakdown   = m.get("score_breakdown", {})
        explanation = m.get("explanation", {})
        cid         = str(m.get("candidate_id", m.get("person_b", "")))
        name        = uid_to_name.get(cid, cid[:8] + "…")
        score       = m.get("score", 0)
        expl_text   = explanation.get("explanation_text", "—") if isinstance(explanation, dict) else str(explanation)

        score_color = "green" if score >= 0.7 else "yellow" if score >= 0.4 else "red"

        table.add_row(
            str(i),
            name,
            f"[{score_color}]{score:.3f}[/{score_color}]",
            f"{breakdown.get('relevance', 0):.2f}",
            f"{breakdown.get('evidence_strength', 0):.2f}",
            f"{breakdown.get('timing', 0):.2f}",
            expl_text[:55],
        )

    console.print(table)

    # Print full explanations below
    console.print()
    for i, m in enumerate(matches, 1):
        explanation = m.get("explanation", {})
        if not isinstance(explanation, dict):
            continue
        cid  = str(m.get("candidate_id", m.get("person_b", "")))
        name = uid_to_name.get(cid, cid[:8] + "…")
        console.print(f"[bold cyan]Match #{i} — {name}[/bold cyan]")
        console.print(f"  [italic]{explanation.get('explanation_text', '—')}[/italic]")
        if explanation.get("opening_question"):
            console.print(f"  [dim]Opening Q: {explanation['opening_question']}[/dim]")
        console.print()

    return matches


# ─────────────────────────────────────────────
#  Step 7 — Submit feedback on top match
# ─────────────────────────────────────────────

async def submit_feedback(client: httpx.AsyncClient, matches: list, requester: dict):
    console.rule("[bold blue]Step 7 · Submit Feedback (top match)")
    if not matches:
        console.print("[yellow]No matches to give feedback on.[/yellow]")
        return

    top = matches[0]
    match_id = top.get("match_id")
    if not match_id:
        console.print("[yellow]Top match has no match_id — skipping feedback.[/yellow]")
        return

    payload = {
        "match_id":      match_id,
        "user_id":       requester["user_id"],
        "feedback_type": "accepted",
        "note":          "Test feedback — auto-submitted by test script",
    }
    result = await post(client, f"/matches/{match_id}/feedback", json={
    "actor_user_id": requester["user_id"],
    "feedback_type": "accepted",
    "payload": {"notes": "Test feedback — auto-submitted by test script"},
    })
    if result:
        console.print(f"  [green]✓[/green] Feedback 'accepted' submitted for match {match_id[:8]}…")
    return result


# ─────────────────────────────────────────────
#  Step 8 — Coverage check
# ─────────────────────────────────────────────

async def print_coverage(client: httpx.AsyncClient):
    console.rule("[bold blue]Step 8 · Coverage Report")
    data = await get(client, f"/analytics/{TENANT_ID}/coverage")
    if not data:
        return

    table = Table(show_header=True, header_style="bold cyan", box=None, padding=(0,1))
    table.add_column("User",          width=22)
    table.add_column("Facts",         width=7)
    table.add_column("Signals",       width=9)
    table.add_column("Open Matches",  width=13)
    table.add_column("Has Facts?",    width=11)
    table.add_column("Has Signal?",   width=12)

    for u in data.get("users", []):
        has_f = "[green]✓[/green]" if u["has_facts"]         else "[red]✗[/red]"
        has_s = "[green]✓[/green]" if u["has_active_signal"] else "[yellow]–[/yellow]"
        table.add_row(
            u["display_name"][:22],
            str(u["fact_count"]),
            str(u["active_signals"]),
            str(u["open_matches"]),
            has_f,
            has_s,
        )

    console.print(table)
    console.print(
        f"\n[bold]Cold-start users (no facts):[/bold] {data.get('cold_start_users', '?')}"
        f"  |  [bold]No signal:[/bold] {data.get('no_signal_users', '?')}"
    )


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

async def main():
    global BASE_URL   # ✅ MOVE THIS TO THE TOP (before ANY usage)

    parser = argparse.ArgumentParser(description="Delllo matchmaking E2E test")
    parser.add_argument("--base-url",     default=BASE_URL,
                        help="API base URL (default: http://localhost:8000)")
    parser.add_argument("--user",         type=int, default=1,
                        help="Dataset row id of the requester (1–10, default: 1)")
    parser.add_argument("--tx-type",      default="technical_problem_solving",
                        choices=TRANSACTION_TYPES,
                        help="Transaction type for match generation")
    parser.add_argument("--skip-ingest",  action="store_true",
                        help="Skip ingest/extract (if data is already loaded)")
    parser.add_argument("--skip-users",   action="store_true",
                        help="Skip user upsert (if users already exist)")
    parser.add_argument("--only-coverage",action="store_true",
                        help="Only print coverage report")

    args = parser.parse_args()

    BASE_URL = args.base_url.rstrip("/")  # ✅ safe now

    console.print(f"\n[bold magenta]Delllo RAIN3.0 — Matchmaking E2E Test[/bold magenta]")
    console.print(f"API: [cyan]{BASE_URL}[/cyan]  |  Tenant: [cyan]{TENANT_ID[:8]}…[/cyan]\n")

    rows  = load_dataset()
    console.print(f"Loaded [bold]{len(rows)}[/bold] users from dataset\n")

    async with httpx.AsyncClient(timeout=300.0) as client:

        # Health check
        await ensure_tenant(client)

        if args.only_coverage:
            await print_coverage(client)
            return

        # Upsert users
        if not args.skip_users:
            users = await upsert_users(client, rows)
        else:
            console.print("[dim]Skipping user upsert (--skip-users)[/dim]")
            users = [{**r, "user_id": user_uuid(int(r["id"])),
                      "bio": json.loads(r["bio_json"])} for r in rows]

        # Pick requester
        requester_row = next((u for u in users if int(u["id"]) == args.user), users[0])
        console.print(
            f"\n[bold]Requester:[/bold] {requester_row['name']} "
            f"(id={args.user}, uid={requester_row['user_id'][:8]}…)\n"
        )

        # Ingest + extract
        if not args.skip_ingest:
            await ingest_and_extract(client, users)
        else:
            console.print("[dim]Skipping ingest (--skip-ingest)[/dim]")

        # Post intent
        await post_intent(client, requester_row)

        # Generate matches
        matches_resp = await generate_matches(client, requester_row, args.tx_type)

        # Print results
        matches = print_match_table(matches_resp, users, requester_row)

        # Submit feedback
        if matches:
            await submit_feedback(client, matches, requester_row)

        # Coverage report
        await print_coverage(client)

    console.print("\n[bold green]✓ Test complete[/bold green]\n")


if __name__ == "__main__":
    asyncio.run(main())