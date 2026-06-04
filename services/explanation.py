"""
Delllo RAIN3.0 — Explanation Service (Phase 2)

Generates policy-safe natural-language match explanations using the local LLM.

Output per match:
  - explanation_text  — why this match matters
  - agenda_text       — suggested meeting agenda
  - opening_question  — suggested first question

Called from matches.py after scoring, stored in the explanations table.
"""

from __future__ import annotations

import json
import logging
import uuid

import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.config import settings
from app.services.ranking import PersonProfile

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  Prompt builder
# ─────────────────────────────────────────────────────────────

def _build_explanation_prompt(
    requester: PersonProfile,
    candidate: PersonProfile,
    score: float,
    score_breakdown: dict,
    transaction_type: str,
) -> str:
    req_skills    = ", ".join(list(requester.skills)[:5]) or "—"
    req_needs     = ", ".join(list(requester.needs)[:3]) or "—"
    req_location  = requester.locations[0] if requester.locations else "unknown"

    can_skills    = ", ".join(list(candidate.skills)[:5]) or "—"
    can_offers    = ", ".join(list(candidate.offers)[:3]) or "—"
    can_location  = candidate.locations[0] if candidate.locations else "unknown"
    can_conf      = round(candidate.avg_confidence, 2)

    same_location = (
        req_location.lower() == can_location.lower()
        if req_location != "unknown" and can_location != "unknown"
        else False
    )
    timing_note = ""
    if requester.has_live_intent:
        timing_note = f"The requester has posted a live intent: \"{requester.intent_text[:100]}\". "
    if candidate.has_presence:
        timing_note += "The candidate is currently present and available. "

    return f"""You are a professional networking assistant at a financial institution.
Generate a concise, professional, policy-safe match explanation for the following pair.

TRANSACTION TYPE: {transaction_type.replace("_", " ")}
MATCH SCORE: {score:.2f}

PERSON A (requester):
- Skills: {req_skills}
- Current needs: {req_needs}
- Location: {req_location}
- Has live intent: {requester.has_live_intent}

PERSON B (candidate):
- Skills: {can_skills}
- Offers: {can_offers}
- Location: {can_location}
- Evidence confidence: {can_conf}
- Same location as requester: {same_location}

SCORE SIGNALS:
- Relevance: {score_breakdown.get('relevance', 0):.2f}
- Complementarity: {score_breakdown.get('complementarity', 0):.2f}
- Evidence strength: {score_breakdown.get('evidence_strength', 0):.2f}
- Timing: {score_breakdown.get('timing', 0):.2f}
{timing_note}

RULES:
1. Do NOT reveal exact skill names that are marked private.
2. Use professional, neutral language appropriate for a financial institution.
3. Keep explanation_text under 60 words.
4. Keep agenda_text as 3 bullet points maximum.
5. opening_question must be a single concrete question under 25 words.
6. Return ONLY valid JSON with exactly these three keys.

Return JSON:
{{
  "explanation_text": "...",
  "agenda_text": "...",
  "opening_question": "..."
}}"""


# ─────────────────────────────────────────────────────────────
#  LLM call
# ─────────────────────────────────────────────────────────────

async def _call_ollama_explanation(prompt: str) -> dict:
    payload = {
        "model": settings.ollama_model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a professional match explanation assistant. "
                    "Return only valid JSON with keys: explanation_text, agenda_text, opening_question."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.3, "num_predict": 512},
    }
    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(f"{settings.ollama_base_url}/api/chat", json=payload)
        resp.raise_for_status()
        raw = resp.json()["message"]["content"]

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        import re
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            return json.loads(match.group())
        raise


# ─────────────────────────────────────────────────────────────
#  Main entry point
# ─────────────────────────────────────────────────────────────

async def generate_and_store_explanation(
    db: AsyncSession,
    *,
    match_id: str,
    requester: PersonProfile,
    candidate: PersonProfile,
    score: float,
    score_breakdown: dict,
    transaction_type: str,
) -> dict:
    """
    Generate a policy-safe explanation for a match and store it in the
    explanations table. Returns the explanation dict.
    Non-fatal: returns a stub if LLM call fails.
    """
    try:
        prompt = _build_explanation_prompt(
            requester=requester,
            candidate=candidate,
            score=score,
            score_breakdown=score_breakdown,
            transaction_type=transaction_type,
        )
        explanation = await _call_ollama_explanation(prompt)
    except Exception as e:
        logger.warning(f"Explanation generation failed for match {match_id[:8]}: {e}")
        explanation = {
            "explanation_text": "Strong expertise match with relevant experience and availability.",
            "agenda_text": "• Discuss current challenge\n• Review relevant experience\n• Agree next steps",
            "opening_question": "What specific aspect of this problem would be most useful to discuss first?",
        }

    explanation_id = str(uuid.uuid4())
    try:
        await db.execute(
            text("""
                INSERT INTO explanations
                    (explanation_id, match_id, explanation_text,
                     agenda_text, opening_question, model_used)
                VALUES
                    (:eid, :mid, :explanation, :agenda, :question, :model)
                ON CONFLICT (match_id) DO UPDATE
                    SET explanation_text = EXCLUDED.explanation_text,
                        agenda_text      = EXCLUDED.agenda_text,
                        opening_question = EXCLUDED.opening_question
            """),
            {
                "eid":         explanation_id,
                "mid":         match_id,
                "explanation": explanation.get("explanation_text", ""),
                "agenda":      explanation.get("agenda_text", ""),
                "question":    explanation.get("opening_question", ""),
                "model":       settings.ollama_model,
            },
        )
        logger.info(f"Explanation stored for match {match_id[:8]}")  
    except Exception as e:
        logger.error(f"Failed to store explanation for match {match_id[:8]}: {e}")

    return explanation