import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import redis
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)


def _merge_transcript_text(existing: str, incoming: str) -> str:
    existing = (existing or "").strip()
    incoming = (incoming or "").strip()
    if not incoming:
        return existing
    if not existing:
        return incoming
    if incoming == existing:
        return existing
    if incoming.startswith(existing):
        return incoming
    if existing.startswith(incoming):
        return existing

    existing_words = existing.split()
    incoming_words = incoming.split()
    max_overlap = min(len(existing_words), len(incoming_words))
    for overlap in range(max_overlap, 0, -1):
        if existing_words[-overlap:] == incoming_words[:overlap]:
            return " ".join(existing_words + incoming_words[overlap:]).strip()

    return f"{existing} {incoming}".strip()


class StateStore:
    def __init__(self) -> None:
        self.host = os.environ.get("REDIS_HOST")
        self.password = os.environ.get("REDIS_KEY")
        self.port = int(os.environ.get("REDIS_PORT"))
        self.ssl = os.environ.get("REDIS_SSL").lower() == "true"
        self.db = int(os.environ.get("REDIS_DB"))
        self.prefix = os.environ.get("REDIS_KEY_PREFIX")
        self.ttl = int(os.environ.get("REDIS_TTL"))


        logger.info(
            "Initializing Redis client host=%s port=%s ssl=%s db=%s prefix=%s ttl=%s",
            self.host,
            self.port,
            self.ssl,
            self.db,
            self.prefix,
            self.ttl,
        )

        self.client = redis.Redis(
            host=self.host,
            port=self.port,
            password=self.password,
            ssl=self.ssl,
            db=self.db,
            decode_responses=True,
        )

        try:
            self.client.ping()
            logger.info("Redis connection successful")
        except Exception:
            logger.exception("Redis connection failed")
            raise

    def _state_key(self, instance_id: str) -> str:
        return f"{self.prefix}{instance_id}"

    def _bot_map_key(self, bot_id: str) -> str:
        return f"{self.prefix}botmap:{bot_id}"

    def _state_key_pattern(self) -> str:
        return f"{self.prefix}*"

    @staticmethod
    def _safe_json_loads(data: Optional[str]) -> Dict[str, Any]:
        if not data:
            return {}
        try:
            parsed = json.loads(data)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            logger.exception("Failed to parse state JSON")
            raise

    def save_state(self, instance_id: str, state: Dict[str, Any]) -> Dict[str, Any]:
        key = self._state_key(instance_id)
        payload = json.dumps(state)
        try:
            self.client.set(key, payload, ex=self.ttl)
            logger.info("SAVE state key=%s payload=%s ttl=%s", key, payload, self.ttl)
            return state
        except Exception:
            logger.exception("SAVE failed key=%s", key)
            raise

    def get_state(self, instance_id: str) -> Dict[str, Any]:
        key = self._state_key(instance_id)
        try:
            raw = self.client.get(key)
            if not raw:
                logger.warning("READ missing key=%s", key)
                return {}
            state = self._safe_json_loads(raw)
            logger.info(
                "READ state key=%s current_issue=%s issues_count=%d status=%s",
                key,
                state.get("current_issue", {}).get("key") if state.get("current_issue") else None,
                len(state.get("issues", [])),
                state.get("status")
)
            return state
        except Exception:
            logger.exception("READ failed key=%s", key)
            raise

    def delete_state(self, instance_id: str) -> None:
        key = self._state_key(instance_id)
        try:
            self.client.delete(key)
            logger.info("DELETE state key=%s", key)
        except Exception:
            logger.exception("DELETE failed key=%s", key)
            raise

    def map_bot_to_instance(self, bot_id: str, instance_id: str) -> None:
        key = self._bot_map_key(bot_id)
        try:
            self.client.set(key, instance_id, ex=self.ttl)
            logger.info("MAP bot_id=%s instance_id=%s", bot_id, instance_id)
        except Exception:
            logger.exception("MAP failed bot_id=%s instance_id=%s", bot_id, instance_id)
            raise

    def get_instance_id_by_bot_id(self, bot_id: str) -> str:
        key = self._bot_map_key(bot_id)
        try:
            value = self.client.get(key) or ""
            logger.info("LOOKUP bot_id=%s found=%s instance_id=%s", bot_id, bool(value), value or None)
            return value
        except Exception:
            logger.exception("LOOKUP failed bot_id=%s", bot_id)
            raise

    def get_reply_state(self, instance_id: str) -> Dict[str, Any]:
        state = self.get_state(instance_id)
        reply_state = state.get("reply_state", {}) if state else {}
        logger.info("GET reply_state instance_id=%s keys=%s", instance_id, list(reply_state.keys()))
        return reply_state

    def clear_reply_window(self, instance_id: str) -> Dict[str, Any]:
        state = self.get_state(instance_id)
        if not state:
            logger.warning("CLEAR reply window skipped missing instance_id=%s", instance_id)
            return {}
        state["reply_state"] = {
            "awaiting_reply": False,
            "pending_bot_completion": False,
            "speaker_name": "",
            "final_segments": [],
            "partial_segments": [],
            "partial_text": "",
            "combined_text": "",
            "last_activity_ts": time.time(),
            "reply_window_started_ts": None,
            "reply_window_opened_at_ts": None,
            "has_final_segment": False,
            "last_event": "reply_window_cleared",
            "finalize_after_ts": None,
            "intent": "normal",
        }
        logger.info("CLEAR reply window instance_id=%s", instance_id)
        return self.save_state(instance_id, state)

    def schedule_reply_window(self, instance_id: str, open_at_ts: float, source_event: str = "bot_prompt_sent") -> Dict[str, Any]:
        state = self.get_state(instance_id)
        if not state:
            logger.warning("SCHEDULE reply window skipped missing instance_id=%s", instance_id)
            return {}
        state["reply_state"] = {
            "awaiting_reply": False,
            "pending_bot_completion": True,
            "speaker_name": "",
            "final_segments": [],
            "partial_segments": [],
            "partial_text": "",
            "combined_text": "",
            "last_activity_ts": time.time(),
            "reply_window_started_ts": time.time(),
            "reply_window_opened_at_ts": float(open_at_ts),
            "has_final_segment": False,
            "last_event": source_event,
            "finalize_after_ts": None,
            "intent": "normal",
        }
        logger.info("SCHEDULE reply window instance_id=%s open_at_ts=%s source_event=%s", instance_id, open_at_ts, source_event)
        return self.save_state(instance_id, state)

    def begin_reply_window(self, instance_id: str) -> Dict[str, Any]:
        state = self.get_state(instance_id)
        if not state:
            logger.warning("BEGIN reply window skipped missing instance_id=%s", instance_id)
            return {}
        now = time.time()
        reply_state = state.get("reply_state", {})
        reply_state.update(
            {
                "awaiting_reply": True,
                "pending_bot_completion": False,
                "speaker_name": "",
                "final_segments": [],
                "partial_segments": [],
                "partial_text": "",
                "combined_text": "",
                "last_activity_ts": now,
                "reply_window_started_ts": reply_state.get("reply_window_started_ts") or now,
                "reply_window_opened_at_ts": now,
                "has_final_segment": False,
                "last_event": "reply_window_opened",
                "finalize_after_ts": None,
                "intent": "normal",
            }
        )
        state["reply_state"] = reply_state
        if state.get("status") != "completed":
            state["status"] = "waiting_for_update"
        logger.info("BEGIN reply window instance_id=%s", instance_id)
        return self.save_state(instance_id, state)

    def activate_ready_reply_windows(self, now: Optional[float] = None) -> List[str]:
        now = float(now or time.time())
        activated: List[str] = []
        for item in self._iter_states():
            instance_id = item["instance_id"]
            state = item["state"]
            reply_state = state.get("reply_state", {})
            open_at = reply_state.get("reply_window_opened_at_ts")
            if not reply_state.get("pending_bot_completion"):
                continue
            if state.get("is_bot_speaking"):
                continue
            if open_at is not None and float(open_at) > now:
                continue
            self.begin_reply_window(instance_id)
            activated.append(instance_id)
        if activated:
            logger.info("ACTIVATE reply windows instances=%s", activated)
        return activated

    def set_bot_speaking(
        self,
        instance_id: str,
        is_speaking: bool,
        *,
        started_at: Optional[float] = None,
        ends_at: Optional[float] = None,
    ) -> Dict[str, Any]:
        state = self.get_state(instance_id)
        if not state:
            logger.warning("SET bot speaking skipped missing instance_id=%s", instance_id)
            return {}
        state["is_bot_speaking"] = bool(is_speaking)
        if started_at is not None:
            state["bot_speaking_started_ts"] = float(started_at)
        if ends_at is not None:
            state["bot_speaking_ends_at_ts"] = float(ends_at)
        if is_speaking:
            state["status"] = "bot_speaking"
        elif state.get("status") != "completed":
            state["status"] = "waiting_for_update"
        logger.info(
            "SET bot speaking instance_id=%s is_speaking=%s started_at=%s ends_at=%s",
            instance_id,
            is_speaking,
            started_at,
            ends_at,
        )
        return self.save_state(instance_id, state)

    def buffer_transcript(
        self,
        instance_id: str,
        speaker_name: str,
        text: str,
        is_final: bool,
        event_name: str,
        intent: str = "normal",
        force_finalize_after_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        state = self.get_state(instance_id)
        if not state:
            logger.warning("BUFFER transcript skipped missing instance_id=%s", instance_id)
            return {}

        now = time.time()
        reply_state = state.get("reply_state", {}) or {}
        final_segments = list(reply_state.get("final_segments") or [])
        partial_segments = list(reply_state.get("partial_segments") or [])
        partial_text = str(reply_state.get("partial_text") or "")
        incoming_text = (text or "").strip()

        if is_final:
            finalized_text = incoming_text or partial_text
            if finalized_text:
                if not final_segments or final_segments[-1] != finalized_text:
                    final_segments.append(finalized_text)
            partial_segments = []
            partial_text = ""
        else:
            merged_partial = _merge_transcript_text(partial_text, incoming_text)
            partial_text = merged_partial
            if incoming_text:
                if not partial_segments or partial_segments[-1] != incoming_text:
                    partial_segments.append(incoming_text)

        combined_parts = [segment.strip() for segment in final_segments if segment and segment.strip()]
        if partial_text.strip() and (not combined_parts or combined_parts[-1] != partial_text.strip()):
            combined_parts.append(partial_text.strip())
        combined_text = " ".join(combined_parts).strip()

        reply_state.update(
            {
                "awaiting_reply": bool(reply_state.get("awaiting_reply") or event_name == "participant_events.speech_on"),
                "pending_bot_completion": bool(reply_state.get("pending_bot_completion", False)),
                "speaker_name": speaker_name or reply_state.get("speaker_name") or "",
                "final_segments": final_segments,
                "partial_segments": partial_segments,
                "partial_text": partial_text,
                "combined_text": combined_text,
                "last_activity_ts": now,
                "reply_window_started_ts": reply_state.get("reply_window_started_ts") or now,
                "reply_window_opened_at_ts": reply_state.get("reply_window_opened_at_ts") or now,
                "has_final_segment": bool(final_segments),
                "last_event": event_name,
                "intent": intent or reply_state.get("intent") or "normal",
            }
        )
        if force_finalize_after_seconds is not None:
            reply_state["finalize_after_ts"] = now + float(force_finalize_after_seconds)
        else:
            reply_state.setdefault("finalize_after_ts", None)

        state["reply_state"] = reply_state
        logger.info(
            "BUFFER transcript instance_id=%s speaker=%s event=%s is_final=%s text_len=%s combined_len=%s",
            instance_id,
            speaker_name,
            event_name,
            is_final,
            len(incoming_text),
            len(combined_text),
        )
        return self.save_state(instance_id, state)

    def consume_reply_buffer(self, instance_id: str) -> Dict[str, Any]:
        state = self.get_state(instance_id)
        if not state:
            logger.warning("CONSUME reply buffer skipped missing instance_id=%s", instance_id)
            return {}
        reply_state = state.get("reply_state", {}) or {}
        combined_text = str(reply_state.get("combined_text") or "").strip()
        speaker_name = str(reply_state.get("speaker_name") or "Unknown")
        intent = str(reply_state.get("intent") or "normal")
        payload = {
            "speaker_name": speaker_name,
            "text": combined_text,
            "intent": intent,
        }
        logger.info(
            "CONSUME reply buffer instance_id=%s speaker=%s text_len=%s intent=%s",
            instance_id,
            speaker_name,
            len(combined_text),
            intent,
        )
        self.clear_reply_window(instance_id)
        return payload

    def list_active_reply_windows(self) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for item in self._iter_states():
            state = item["state"]
            reply_state = state.get("reply_state", {}) or {}
            if reply_state.get("awaiting_reply"):
                results.append({"instance_id": item["instance_id"], "reply_state": reply_state})
        logger.info("LIST active reply windows count=%s", len(results))
        return results

    def _iter_states(self) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        try:
            for key in self.client.scan_iter(match=self._state_key_pattern()):
                if ":botmap:" in key:
                    continue
                instance_id = key.replace(self.prefix, "", 1)
                raw = self.client.get(key)
                if not raw:
                    continue
                state = self._safe_json_loads(raw)
                if state:
                    items.append({"instance_id": instance_id, "state": state})
        except Exception:
            logger.exception("ITER states failed")
            raise
        return items
