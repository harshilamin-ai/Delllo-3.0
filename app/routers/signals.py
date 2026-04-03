"""
Delllo — Signals Router

POST /v1/signals/intent      Record a live intent signal → Postgres + sKG
POST /v1/signals/presence    Record a presence signal   → Postgres + sKG
POST /v1/signals/availability Record availability window
"""

import json
import logging
from uuid import UUID
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db.postgres import get_db
from app.db.graph import get_driver
from app.services import graph_writer

logger = logging.getLogger(__name__)
router = APIRouter()


# ─────────────────────────────────────────────
#  Request schemas
# ─────────────────────────────────────────────

class SignalCreate(BaseModel):
    tenant_id: UUID
    user_id: UUID
    signal_type: str          # intent | presence | urgency | availability
    payload: Dict[str, Any] = {}
    valid_to: Optional[str] = None


# ─────────────────────────────────────────────
#  POST /v1/signals/intent
# ─────────────────────────────────────────────

@router.post("/signals/intent")
async def post_intent(signal: SignalCreate, db: AsyncSession = Depends(get_db)):
    """
    Record a live intent signal.
    Example payload: {"text": "Need HY bond liquidity expertise today", "urgency": "high"}
    Expires any previous active intent for this user first.
    Mirrors to Memgraph sKG as a LiveIntent node.
    """
    uid = str(signal.user_id)
    tid = str(signal.tenant_id)
    payload_json = json.dumps(signal.payload)

    # Step 1: expire existing active intents for this user (separate statement)
    await db.execute(
        text("""
            UPDATE live_signals
            SET valid_to = NOW()
            WHERE user_id = :uid AND signal_type = 'intent' AND valid_to IS NULL
        """),
        {"uid": uid},
    )

    # Step 2: insert new intent, return signal_id for sKG write
    result = await db.execute(
        text("""
            INSERT INTO live_signals (tenant_id, user_id, signal_type, payload_json, valid_to)
            VALUES (:tid, :uid, 'intent', CAST(:payload AS JSONB), :valid_to)
            RETURNING signal_id
        """),
        {"tid": tid, "uid": uid, "payload": payload_json, "valid_to": signal.valid_to},
    )
    row = result.mappings().first()
    signal_id = str(row["signal_id"])

    # Step 3: mirror to sKG (non-fatal if Memgraph is down)
    intent_text = signal.payload.get("text", "")
    if intent_text:
        try:
            await graph_writer.upsert_live_intent(
                get_driver(),
                person_id=uid,
                tenant_id=tid,
                signal_id=signal_id,
                intent_text=intent_text,
                valid_to=signal.valid_to,
            )
        except Exception as e:
            logger.warning(f"sKG intent write failed (non-fatal): {e}")

    return {
        "status": "accepted",
        "signal_type": "intent",
        "signal_id": signal_id,
    }


# ─────────────────────────────────────────────
#  POST /v1/signals/presence
# ─────────────────────────────────────────────

@router.post("/signals/presence")
async def post_presence(signal: SignalCreate, db: AsyncSession = Depends(get_db)):
    """
    Record a presence signal — e.g. badged into Amsterdam HQ Floor 7.
    Expected payload: {"location": "Amsterdam HQ", "floor": "7"}
    Mirrors to Memgraph sKG as a Presence node.
    """
    uid = str(signal.user_id)
    tid = str(signal.tenant_id)
    payload_json = json.dumps(signal.payload)

    result = await db.execute(
        text("""
            INSERT INTO live_signals (tenant_id, user_id, signal_type, payload_json, valid_to)
            VALUES (:tid, :uid, 'presence', CAST(:payload AS JSONB), :valid_to)
            RETURNING signal_id
        """),
        {"tid": tid, "uid": uid, "payload": payload_json, "valid_to": signal.valid_to},
    )
    row = result.mappings().first()
    signal_id = str(row["signal_id"])

    # Mirror to sKG
    location = signal.payload.get("location", "")
    floor = signal.payload.get("floor")
    if location:
        try:
            await graph_writer.upsert_presence(
                get_driver(),
                person_id=uid,
                tenant_id=tid,
                signal_id=signal_id,
                location=location,
                floor=floor,
                valid_to=signal.valid_to,
            )
        except Exception as e:
            logger.warning(f"sKG presence write failed (non-fatal): {e}")

    return {
        "status": "accepted",
        "signal_type": "presence",
        "signal_id": signal_id,
    }


# ─────────────────────────────────────────────
#  POST /v1/signals/availability
# ─────────────────────────────────────────────

@router.post("/signals/availability")
async def post_availability(signal: SignalCreate, db: AsyncSession = Depends(get_db)):
    """
    Record an availability window.
    Expected payload: {"available_until": "14:30", "mode": "in_person"}
    """
    uid = str(signal.user_id)
    tid = str(signal.tenant_id)
    payload_json = json.dumps(signal.payload)

    result = await db.execute(
        text("""
            INSERT INTO live_signals (tenant_id, user_id, signal_type, payload_json, valid_to)
            VALUES (:tid, :uid, 'availability', CAST(:payload AS JSONB), :valid_to)
            RETURNING signal_id
        """),
        {"tid": tid, "uid": uid, "payload": payload_json, "valid_to": signal.valid_to},
    )
    row = result.mappings().first()
    signal_id = str(row["signal_id"])

    return {
        "status": "accepted",
        "signal_type": "availability",
        "signal_id": signal_id,
    }


# ─────────────────────────────────────────────
#  POST /v1/signals/meeting-outcome
# ─────────────────────────────────────────────

@router.post("/signals/meeting-outcome")
async def post_meeting_outcome(signal: SignalCreate, db: AsyncSession = Depends(get_db)):
    """
    Record post-meeting outcome signal.
    Expected payload: {"match_id": "...", "met": true, "quality_score": 4, "notes": "..."}
    Also writes an InteractionOutcome to oKG.
    """
    uid = str(signal.user_id)
    tid = str(signal.tenant_id)
    payload_json = json.dumps(signal.payload)

    result = await db.execute(
        text("""
            INSERT INTO live_signals (tenant_id, user_id, signal_type, payload_json, valid_to)
            VALUES (:tid, :uid, 'meeting_outcome', CAST(:payload AS JSONB), NULL)
            RETURNING signal_id
        """),
        {"tid": tid, "uid": uid, "payload": payload_json},
    )
    row = result.mappings().first()
    signal_id = str(row["signal_id"])

    # Write oKG outcome if a match_id is provided
    match_id = signal.payload.get("match_id")
    if match_id:
        met = signal.payload.get("met", False)
        quality_score = signal.payload.get("quality_score")
        outcome_type = "met" if met else "no_show"

        try:
            import uuid as _uuid
            outcome_id = f"outcome_{str(_uuid.uuid4())[:8]}"
            await graph_writer.upsert_interaction_outcome(
                get_driver(),
                match_id=match_id,
                outcome_id=outcome_id,
                outcome_type=outcome_type,
                quality_score=float(quality_score) if quality_score else None,
            )
        except Exception as e:
            logger.warning(f"oKG outcome write failed (non-fatal): {e}")

    return {
        "status": "accepted",
        "signal_type": "meeting_outcome",
        "signal_id": signal_id,
    }