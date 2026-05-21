"""
Delllo RAIN3.0 — Policy Engine (Phase 2)

Enforces field-level visibility at every data access point.

Visibility classes (from design doc):
  private              — only visible to the user themselves
  match_engine_only    — used by ranking but never shown in UI
  tenant_discoverable  — visible to all users in the same tenant
  mutual_match_only    — only visible when both parties have accepted
  public_event_only    — visible in event contexts only

Enforcement points (as per spec):
  1. ingestion           — tag facts on write
  2. extraction output   — already handled via visibility field
  3. graph write         — already stored on nodes
  4. retrieval           — filter candidates
  5. ranking inputs      — allow private facts in score, not in output
  6. explanation         — redact sensitive specifics
  7. API response        — call sanitise_profile() before returning

Usage:
    from app.services.policy import PolicyEngine
    engine = PolicyEngine(viewer_user_id="u123", viewer_tenant_id="t456")
    safe_facts = engine.filter_facts(facts, owner_user_id="u789")
    safe_profile = engine.sanitise_profile(profile_dict, owner_user_id="u789")
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  Visibility hierarchy
#  Higher = more visible
# ─────────────────────────────────────────────────────────────

VISIBILITY_RANK: dict[str, int] = {
    "private":             0,
    "match_engine_only":   1,
    "mutual_match_only":   2,
    "public_event_only":   3,
    "tenant_discoverable": 4,
}

# What the UI (non-ranking) may expose at each context
UI_ALLOWED: dict[str, set[str]] = {
    "self":         {"private", "match_engine_only", "mutual_match_only",
                     "public_event_only", "tenant_discoverable"},
    "mutual_match": {"mutual_match_only", "public_event_only", "tenant_discoverable"},
    "tenant":       {"public_event_only", "tenant_discoverable"},
    "event":        {"public_event_only", "tenant_discoverable"},
    "public":       {"tenant_discoverable"},
}

# Redaction placeholder shown when a fact exists but isn't visible
REDACTED_LABEL = "[expertise available — contact for details]"


# ─────────────────────────────────────────────────────────────
#  Policy Engine
# ─────────────────────────────────────────────────────────────

class PolicyEngine:
    def __init__(
        self,
        *,
        viewer_user_id: str,
        viewer_tenant_id: str,
        match_accepted: bool = False,
        is_event_context: bool = False,
    ):
        self.viewer_user_id   = viewer_user_id
        self.viewer_tenant_id = viewer_tenant_id
        self.match_accepted   = match_accepted
        self.is_event_context = is_event_context

    def _context(self, owner_user_id: str) -> str:
        if owner_user_id == self.viewer_user_id:
            return "self"
        if self.match_accepted:
            return "mutual_match"
        if self.is_event_context:
            return "event"
        return "tenant"

    def can_see(self, visibility: str, owner_user_id: str) -> bool:
        """Return True if the viewer may see a fact with this visibility."""
        ctx     = self._context(owner_user_id)
        allowed = UI_ALLOWED.get(ctx, set())
        return visibility in allowed

    def can_use_for_ranking(self, visibility: str) -> bool:
        """
        Ranking engine may use all facts except 'private'.
        The score itself is never exposed; only the breakdown is.
        """
        return visibility != "private"

    def filter_facts(
        self,
        facts: list[dict],
        owner_user_id: str,
    ) -> list[dict]:
        """
        Filter a list of fact dicts for API response.
        Facts the viewer may not see are dropped entirely
        (not redacted, since the fact's existence is itself information).
        """
        ctx     = self._context(owner_user_id)
        allowed = UI_ALLOWED.get(ctx, set())
        return [f for f in facts if f.get("visibility", "match_engine_only") in allowed]

    def sanitise_profile(
        self,
        profile: dict[str, Any],
        owner_user_id: str,
    ) -> dict[str, Any]:
        """
        Sanitise a profile dict for API output.
        - Removes 'private' and 'match_engine_only' facts from lists.
        - Replaces their raw values with REDACTED_LABEL in skill/domain lists
          so the UI can show "expertise available" without revealing specifics.
        - Leaves other fields intact.
        """
        ctx     = self._context(owner_user_id)
        allowed = UI_ALLOWED.get(ctx, set())

        result = dict(profile)

        for list_key in ("skills", "domains", "offers", "objectives",
                         "achievements", "topics", "needs", "assets",
                         "projects", "locations", "constraints"):
            if list_key not in result:
                continue
            items    = result[list_key]
            filtered = []
            redacted_count = 0

            for item in items:
                vis = item.get("visibility", "match_engine_only")
                if vis in allowed:
                    filtered.append(item)
                else:
                    redacted_count += 1

            if redacted_count > 0:
                filtered.append({
                    "name":       REDACTED_LABEL,
                    "visibility": "redacted",
                    "count":      redacted_count,
                })
            result[list_key] = filtered

        return result

    def sanitise_explanation(self, explanation_text: str) -> str:
        """
        Ensure the explanation text doesn't contain raw private skill names.
        Simple pass-through for now; Phase 3 adds NER-based redaction.
        """
        return explanation_text

    def filter_match_response(
        self,
        match: dict[str, Any],
        owner_user_id: str,
    ) -> dict[str, Any]:
        """
        Sanitise a match response dict.
        The score breakdown is always included (it's numeric, not fact text).
        The candidate's raw fact names are never in match responses anyway.
        """
        result = dict(match)
        # Never expose match_engine_only facts in the response
        # Score breakdown is numeric — safe to show
        if not self.can_see("match_engine_only", owner_user_id):
            result.pop("internal_features", None)
        return result


# ─────────────────────────────────────────────────────────────
#  Standalone helpers (for use without instantiating the class)
# ─────────────────────────────────────────────────────────────

def filter_facts_for_ranking(facts: list[dict]) -> list[dict]:
    """
    Return facts usable by the ranking engine.
    Includes match_engine_only; excludes nothing except truly private.
    """
    return [f for f in facts if f.get("visibility", "match_engine_only") != "private"]


def get_visibility_for_new_fact(
    fact_type: str,
    source_type: str = "cv",
) -> str:
    """
    Assign a default visibility when a new fact is extracted.
    Sensitive types default to match_engine_only; locations are discoverable.
    """
    if fact_type in ("constraint", "need"):
        return "match_engine_only"
    if fact_type == "location":
        return "tenant_discoverable"
    if source_type in ("chat", "meeting_note"):
        return "match_engine_only"
    return "match_engine_only"   # safe default everywhere