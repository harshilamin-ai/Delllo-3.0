"""
Delllo RAIN3.0 — Ranking Engine (Phase 2)

Replaces the fact_count stub with a real deterministic weighted score.

MatchScore =
  w1 * Relevance           (0.24)  — skill/need overlap
+ w2 * Complementarity     (0.16)  — A's needs met by B's offers
+ w3 * Timing              (0.14)  — live intent + presence signals
+ w4 * Proximity           (0.10)  — location match
+ w5 * EvidenceStrength    (0.14)  — confidence of candidate's facts
+ w6 * OutcomeLikelihood   (0.10)  — historical meeting success rate
+ w7 * Novelty             (0.06)  — penalise repeated dead-end matches
- w8 * PrivacyRisk         (0.04)  — exposure of sensitive capabilities
- w9 * InteractionFriction (0.06)  — constraint conflicts + no-show history
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  Weights  (must sum to ±1.0 accounting for sign)
# ─────────────────────────────────────────────────────────────

WEIGHTS: dict[str, float] = {
    "relevance":            0.24,
    "complementarity":      0.16,
    "timing":               0.14,
    "proximity":            0.10,
    "evidence_strength":    0.14,
    "outcome_likelihood":   0.10,
    "novelty":              0.06,
    "privacy_risk":        -0.04,
    "interaction_friction":-0.06,
}


# ─────────────────────────────────────────────────────────────
#  Data containers
# ─────────────────────────────────────────────────────────────

@dataclass
class PersonProfile:
    user_id: str
    # Sets of canonical fact values per type
    skills:      set[str] = field(default_factory=set)
    domains:     set[str] = field(default_factory=set)
    needs:       set[str] = field(default_factory=set)
    objectives:  set[str] = field(default_factory=set)
    offers:      set[str] = field(default_factory=set)
    constraints: list[str] = field(default_factory=list)
    locations:   list[str] = field(default_factory=list)
    # Confidence stats
    avg_confidence:   float = 0.5
    high_conf_count:  int   = 0    # facts with confidence >= 0.8
    total_facts:      int   = 0
    private_facts:    int   = 0
    # Signal state
    has_live_intent:  bool  = False
    has_presence:     bool  = False
    intent_text:      str   = ""
    presence_site:    str   = ""


@dataclass
class ScoreBreakdown:
    relevance:            float = 0.0
    complementarity:      float = 0.0
    timing:               float = 0.0
    proximity:            float = 0.0
    evidence_strength:    float = 0.0
    outcome_likelihood:   float = 0.0
    novelty:              float = 0.0
    privacy_risk:         float = 0.0
    interaction_friction: float = 0.0
    final_score:          float = 0.0

    def to_dict(self) -> dict:
        return {
            "relevance":            round(self.relevance, 4),
            "complementarity":      round(self.complementarity, 4),
            "timing":               round(self.timing, 4),
            "proximity":            round(self.proximity, 4),
            "evidence_strength":    round(self.evidence_strength, 4),
            "outcome_likelihood":   round(self.outcome_likelihood, 4),
            "novelty":              round(self.novelty, 4),
            "privacy_risk":         round(self.privacy_risk, 4),
            "interaction_friction": round(self.interaction_friction, 4),
            "final_score":          round(self.final_score, 4),
        }


# ─────────────────────────────────────────────────────────────
#  Profile loader
# ─────────────────────────────────────────────────────────────

async def load_profile(db: AsyncSession, user_id: str, tenant_id: str) -> PersonProfile:
    """Load all facts and signals for a user into a PersonProfile."""
    profile = PersonProfile(user_id=user_id)

    # ── Facts ────────────────────────────────────────────────
    facts_result = await db.execute(
        text("""
            SELECT fact_type, canonical_value, confidence, visibility
            FROM extracted_facts
            WHERE user_id = :uid AND tenant_id = :tid
        """),
        {"uid": user_id, "tid": tenant_id},
    )
    facts = facts_result.mappings().all()

    conf_sum = 0.0
    for f in facts:
        ft  = f["fact_type"]
        can = f["canonical_value"]
        conf = float(f["confidence"] or 0.5)
        vis  = f["visibility"] or "match_engine_only"

        profile.total_facts += 1
        conf_sum += conf
        if conf >= 0.8:
            profile.high_conf_count += 1
        if vis == "private":
            profile.private_facts += 1

        if ft == "skill":
            profile.skills.add(can)
        elif ft == "domain":
            profile.domains.add(can)
        elif ft == "need":
            profile.needs.add(can)
        elif ft == "objective":
            profile.objectives.add(can)
        elif ft == "offer":
            profile.offers.add(can)
        elif ft == "constraint":
            profile.constraints.append(can)
        elif ft == "location":
            profile.locations.append(can)

    if profile.total_facts:
        profile.avg_confidence = conf_sum / profile.total_facts

    # ── Signals ──────────────────────────────────────────────
    signals_result = await db.execute(
        text("""
            SELECT signal_type, payload_json
            FROM live_signals
            WHERE user_id = :uid
              AND (valid_to IS NULL OR valid_to > NOW())
            ORDER BY created_at DESC
        """),
        {"uid": user_id},
    )
    for sig in signals_result.mappings().all():
        payload = sig["payload_json"] or {}
        if sig["signal_type"] == "intent":
            profile.has_live_intent = True
            profile.intent_text = payload.get("text", "")
        elif sig["signal_type"] == "presence":
            profile.has_presence = True
            profile.presence_site = payload.get("location", "")

    return profile


# ─────────────────────────────────────────────────────────────
#  Feature helpers
# ─────────────────────────────────────────────────────────────

def _clamp(v: float) -> float:
    return max(0.0, min(1.0, v))


def _jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 0.0
    union = a | b
    return len(a & b) / len(union) if union else 0.0


def _keyword_overlap(text_a: str, text_b: str) -> float:
    """Simple word-level overlap for intent text vs skill names."""
    if not text_a or not text_b:
        return 0.0
    words_a = set(re.findall(r"[a-z]{3,}", text_a.lower()))
    words_b = set(re.findall(r"[a-z]{3,}", text_b.lower()))
    stopwords = {"the", "and", "for", "with", "that", "this", "have", "from",
                 "are", "was", "our", "can", "help", "need", "want", "more"}
    words_a -= stopwords
    words_b -= stopwords
    return _jaccard(words_a, words_b)


def _constraints_conflict(a: list[str], b: list[str]) -> bool:
    """
    Detect obvious timing conflicts in constraint text.
    morning_only vs afternoon_only = conflict.
    """
    a_morning = any("morning" in c for c in a)
    a_afternoon = any("afternoon" in c for c in a)
    b_morning = any("morning" in c for c in b)
    b_afternoon = any("afternoon" in c for c in b)

    if (a_morning and b_afternoon) or (a_afternoon and b_morning):
        return True
    return False


# ─────────────────────────────────────────────────────────────
#  Feature computations
# ─────────────────────────────────────────────────────────────

def compute_relevance(requester: PersonProfile, candidate: PersonProfile) -> float:
    """
    How closely candidate's expertise matches requester's needs/objectives.
    Combines skill/domain overlap + intent text keyword match.
    """
    # What requester is looking for
    requester_seeking = requester.needs | requester.objectives
    # What candidate can offer
    candidate_offering = candidate.skills | candidate.domains | candidate.offers

    # Direct canonical overlap
    skill_overlap = _jaccard(
        requester.skills | requester.domains,
        candidate.skills | candidate.domains,
    )
    need_match = (
        len(requester_seeking & candidate_offering) / max(len(requester_seeking), 1)
        if requester_seeking else skill_overlap
    )

    # Live intent text → candidate skill name keyword overlap
    intent_bonus = 0.0
    if requester.intent_text:
        all_candidate_text = " ".join(candidate.skills | candidate.domains | candidate.offers)
        intent_bonus = _keyword_overlap(requester.intent_text, all_candidate_text) * 0.3

    return _clamp(need_match * 0.7 + skill_overlap * 0.3 + intent_bonus)


def compute_complementarity(requester: PersonProfile, candidate: PersonProfile) -> float:
    """
    Whether the pairing creates value rather than two similar people.
    High when A has need and B has matching offer.
    Penalised when both have identical skills (no exchange value).
    """
    requester_seeking  = requester.needs | requester.objectives
    candidate_offering = candidate.skills | candidate.offers | candidate.domains

    # Forward: does B cover A's gaps?
    forward = (
        len(requester_seeking & candidate_offering) / max(len(requester_seeking), 1)
        if requester_seeking else 0.3
    )

    # Reverse: does A cover B's needs?
    requester_offering  = requester.skills | requester.offers | requester.domains
    candidate_seeking   = candidate.needs | candidate.objectives
    reverse = (
        len(candidate_seeking & requester_offering) / max(len(candidate_seeking), 1)
        if candidate_seeking else 0.0
    )

    # Similarity penalty — too-similar pairs rarely create value
    similarity = _jaccard(
        requester.skills | requester.domains,
        candidate.skills | candidate.domains,
    )
    similarity_penalty = similarity * 0.2

    raw = (forward * 0.6 + reverse * 0.4) - similarity_penalty
    return _clamp(raw)


def compute_timing(requester: PersonProfile, candidate: PersonProfile) -> float:
    """
    Whether the need is live right now.
    Both sides having active signals = maximum timing fit.
    """
    score = 0.2  # baseline — even without signals there's some timing potential

    if requester.has_live_intent:
        score += 0.4  # requester is actively looking
    if candidate.has_presence:
        score += 0.3  # candidate is physically available
    if requester.has_presence and candidate.has_presence:
        score += 0.1  # both are present — bonus

    return _clamp(score)


def compute_proximity(requester: PersonProfile, candidate: PersonProfile) -> float:
    """
    Physical or organisational closeness.
    Same site = strong boost; no data = neutral.
    """
    if not requester.locations or not candidate.locations:
        return 0.3  # unknown — neutral, don't penalise

    req_site = requester.locations[0].lower() if requester.locations else ""
    can_site  = candidate.locations[0].lower() if candidate.locations else ""

    if not req_site or not can_site:
        return 0.3

    # Exact site match
    if req_site == can_site:
        return 1.0

    # Partial match (e.g. both "amsterdam" even if different buildings)
    req_words = set(req_site.split())
    can_words = set(can_site.split())
    if req_words & can_words:
        return 0.65

    # Presence signal boosts proximity even without matching locations
    if requester.has_presence and candidate.has_presence:
        return 0.5

    return 0.2  # different locations


def compute_evidence_strength(candidate: PersonProfile) -> float:
    """
    How strongly the candidate's claimed expertise is supported.
    Combines average confidence, high-confidence fact ratio, and total volume.
    """
    if candidate.total_facts == 0:
        return 0.0

    high_conf_ratio = candidate.high_conf_count / candidate.total_facts
    volume_score    = _clamp(candidate.total_facts / 20)  # saturates at 20 facts

    return _clamp(
        candidate.avg_confidence * 0.5
        + high_conf_ratio        * 0.35
        + volume_score           * 0.15
    )


async def compute_outcome_likelihood(
    db: AsyncSession,
    requester_id: str,
    candidate_id: str,
    tenant_id: str,
) -> float:
    """
    Predicted chance this interaction creates value.
    Uses candidate's historical meeting success rate across all matches.
    """
    # Check if these two have met before with a good outcome
    prior_result = await db.execute(
        text("""
            SELECT fe.feedback_type
            FROM feedback_events fe
            JOIN matches m ON m.match_id = fe.match_id
            WHERE m.tenant_id = :tid
              AND ((m.person_a = :a AND m.person_b = :b)
                OR (m.person_a = :b AND m.person_b = :a))
            ORDER BY fe.created_at DESC
            LIMIT 5
        """),
        {"tid": tenant_id, "a": requester_id, "b": candidate_id},
    )
    prior = [r["feedback_type"] for r in prior_result.mappings().all()]

    if prior:
        # They've interacted before
        useful_count = sum(1 for f in prior if f in ("met", "useful"))
        bad_count    = sum(1 for f in prior if f in ("dismissed", "no_show", "not_useful"))
        if bad_count > useful_count:
            return 0.2   # poor prior history
        if useful_count > 0:
            return 0.85  # proven value

    # Candidate's overall meeting rate across all matches
    rate_result = await db.execute(
        text("""
            SELECT
                COUNT(CASE WHEN fe.feedback_type IN ('met','useful') THEN 1 END)::float
                    / NULLIF(COUNT(fe.feedback_id), 0) AS meeting_rate
            FROM feedback_events fe
            JOIN matches m ON m.match_id = fe.match_id
            WHERE (m.person_a = :cid OR m.person_b = :cid)
              AND m.tenant_id = :tid
        """),
        {"cid": candidate_id, "tid": tenant_id},
    )
    row = rate_result.mappings().first()
    meeting_rate = row["meeting_rate"] if row and row["meeting_rate"] is not None else None

    if meeting_rate is None:
        return 0.5   # no history at all — neutral

    return _clamp(0.3 + meeting_rate * 0.7)


async def compute_novelty(
    db: AsyncSession,
    requester_id: str,
    candidate_id: str,
    tenant_id: str,
) -> float:
    """
    Avoid suggesting the same people unless prior outcomes were strong.
    Never matched = 1.0; recently dismissed = 0.1.
    """
    history_result = await db.execute(
        text("""
            SELECT m.status, m.created_at,
                   MAX(fe.feedback_type) AS last_feedback
            FROM matches m
            LEFT JOIN feedback_events fe ON fe.match_id = m.match_id
            WHERE m.tenant_id = :tid
              AND m.person_a = :a AND m.person_b = :b
            GROUP BY m.match_id, m.status, m.created_at
            ORDER BY m.created_at DESC
            LIMIT 1
        """),
        {"tid": tenant_id, "a": requester_id, "b": candidate_id},
    )
    row = history_result.mappings().first()

    if not row:
        return 1.0  # never suggested — fully novel

    status       = row["status"]
    last_feedback = row["last_feedback"]

    if status == "dismissed":
        return 0.1
    if last_feedback in ("not_useful", "no_show"):
        return 0.2
    if last_feedback in ("met", "useful"):
        return 0.6   # good outcome but suppress slight repeat
    if status == "accepted":
        return 0.4   # accepted but no outcome yet
    return 0.5       # recommended, no action


def compute_privacy_risk(candidate: PersonProfile) -> float:
    """
    Penalty if surfacing this match exposes sensitive capabilities.
    Proportional to the fraction of candidate's facts that are private.
    Kept small by design — privacy is enforced at display time too.
    """
    if candidate.total_facts == 0:
        return 0.0
    private_ratio = candidate.private_facts / candidate.total_facts
    return _clamp(private_ratio * 0.5)  # max 0.5 penalty input


def compute_interaction_friction(
    requester: PersonProfile,
    candidate: PersonProfile,
) -> float:
    """
    Penalty for constraints that make meeting hard.
    Timing conflicts, no-show history factored into outcome_likelihood.
    """
    friction = 0.0

    # Constraint timing conflict
    if _constraints_conflict(requester.constraints, candidate.constraints):
        friction += 0.7

    # No location data on either side is mild friction
    if not requester.locations and not candidate.locations:
        friction += 0.2

    return _clamp(friction)


# ─────────────────────────────────────────────────────────────
#  Main scorer
# ─────────────────────────────────────────────────────────────

async def score_pair(
    db: AsyncSession,
    requester: PersonProfile,
    candidate: PersonProfile,
    tenant_id: str,
) -> ScoreBreakdown:
    """
    Compute all 9 features and return a ScoreBreakdown.
    All features are in [0, 1]; weights are applied with their signs.
    """
    bd = ScoreBreakdown()

    bd.relevance            = compute_relevance(requester, candidate)
    bd.complementarity      = compute_complementarity(requester, candidate)
    bd.timing               = compute_timing(requester, candidate)
    bd.proximity            = compute_proximity(requester, candidate)
    bd.evidence_strength    = compute_evidence_strength(candidate)
    bd.outcome_likelihood   = await compute_outcome_likelihood(
        db, requester.user_id, candidate.user_id, tenant_id
    )
    bd.novelty              = await compute_novelty(
        db, requester.user_id, candidate.user_id, tenant_id
    )
    bd.privacy_risk         = compute_privacy_risk(candidate)
    bd.interaction_friction = compute_interaction_friction(requester, candidate)

    bd.final_score = _clamp(
        WEIGHTS["relevance"]            * bd.relevance
        + WEIGHTS["complementarity"]    * bd.complementarity
        + WEIGHTS["timing"]             * bd.timing
        + WEIGHTS["proximity"]          * bd.proximity
        + WEIGHTS["evidence_strength"]  * bd.evidence_strength
        + WEIGHTS["outcome_likelihood"] * bd.outcome_likelihood
        + WEIGHTS["novelty"]            * bd.novelty
        + WEIGHTS["privacy_risk"]       * bd.privacy_risk         # negative weight
        + WEIGHTS["interaction_friction"] * bd.interaction_friction  # negative weight
    )

    logger.debug(
        f"Score {requester.user_id[:8]}→{candidate.user_id[:8]}: "
        f"final={bd.final_score:.3f} "
        f"rel={bd.relevance:.2f} comp={bd.complementarity:.2f} "
        f"timing={bd.timing:.2f} prox={bd.proximity:.2f} "
        f"evid={bd.evidence_strength:.2f} outc={bd.outcome_likelihood:.2f} "
        f"nov={bd.novelty:.2f} priv={bd.privacy_risk:.2f} "
        f"fric={bd.interaction_friction:.2f}"
    )

    return bd


# ─────────────────────────────────────────────────────────────
#  Candidate ranker — ranks a list of candidate user_ids
# ─────────────────────────────────────────────────────────────

async def rank_candidates(
    db: AsyncSession,
    requester_id: str,
    tenant_id: str,
    candidate_ids: list[str],
    min_score: float = 0.05,
) -> list[tuple[str, ScoreBreakdown]]:
    """
    Load profiles for all candidates, score each against the requester,
    return sorted list of (candidate_id, ScoreBreakdown) descending by score.
    Candidates below min_score are filtered out.
    """
    requester = await load_profile(db, requester_id, tenant_id)

    scored: list[tuple[str, ScoreBreakdown]] = []

    for cid in candidate_ids:
        candidate = await load_profile(db, cid, tenant_id)
        breakdown = await score_pair(db, requester, candidate, tenant_id)
        if breakdown.final_score >= min_score:
            scored.append((cid, breakdown))

    scored.sort(key=lambda x: x[1].final_score, reverse=True)
    return scored