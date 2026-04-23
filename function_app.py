import json
import logging
import os
import sys
import threading
import time
from typing import Any, Dict, Optional

import azure.functions as func

from orchestrator import Orchestrator
from state_store import StateStore


def _configure_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s", force=True)
    else:
        root.setLevel(level)
        for handler in root.handlers:
            handler.setLevel(level)
            try:
                handler.flush = getattr(handler, "flush", lambda: None)
            except Exception:
                pass
    try:
        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)
    except Exception:
        pass


_configure_logging()
logger = logging.getLogger(__name__)

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
_orch: Optional[Orchestrator] = None
_store: Optional[StateStore] = None
BOT_SPEAKER_NAME = os.getenv("RECALL_BOT_NAME").strip().lower()
MIN_TRANSCRIPT_LENGTH = int(os.getenv("RECALL_MIN_TRANSCRIPT_LENGTH", "6"))
SILENCE_TIMEOUT_SECONDS = float(os.getenv("STANDUP_SILENCE_TIMEOUT_SECONDS", "3"))
NO_RESPONSE_TIMEOUT_SECONDS = float(os.getenv("STANDUP_NO_RESPONSE_TIMEOUT_SECONDS", "15"))
EARLY_FINALIZE_SECONDS = float(os.getenv("STANDUP_EARLY_FINALIZE_SECONDS", "1.0"))
_MONITOR_STARTED = False
_WARMED_UP = False
_WARMUP_LOCK = threading.Lock()


END_KEYWORDS = [
    "that's it",
    "thats it",
    "that's all",
    "thats all",
    "move on",
    "nothing else",
    "all good",
    "nothing from me",
]
DONE_KEYWORDS = [
    "done",
    "finished",
    "completed",
    "resolved",
    "closed",
    "merged",
    "deployed",
]
BLOCKER_KEYWORDS = [
    "blocked",
    "stuck",
    "waiting on",
    "dependency",
    "can't proceed",
    "cannot proceed",
    "need help",
    "issue with",
]
SKIP_KEYWORDS = [
    "skip",
    "pass",
    "no update",
    "nothing",
]


def _contains_any_phrase(text: str, phrases: list[str]) -> bool:
    lowered = (text or "").lower()
    return any(phrase in lowered for phrase in phrases)


def _detect_explicit_status_intent(text: str) -> str:
    lowered = (text or "").lower().strip()

    explicit_blocked = [
        "move to blocked", "move to block", "move this to blocked", "move this to block",
        "mark blocked", "mark as blocked", "mark this blocked", "set to blocked", "set blocked",
        "change to blocked", "put this on blocked", "put it on blocked",
        "this is blocked", "it is blocked", "blocked",
    ]
    explicit_in_review = [
        "move to in review", "move to review", "mark in review", "mark as in review",
        "set to in review", "change to in review", "ready for review", "for review",
        "under review", "in review",
    ]
    explicit_in_progress = [
        "move to in progress", "mark in progress", "mark as in progress",
        "set to in progress", "change to in progress", "keep in progress",
        "still in progress", "back to in progress", "in progress",
    ]
    explicit_done = [
        "move to done", "mark done", "mark as done", "set to done",
        "change to done", "this is done", "it is done", "move to resolved",
        "mark resolved", "move to closed", "mark closed", "done", "completed",
        "finished", "resolved", "closed",
    ]

    if _contains_any_phrase(lowered, explicit_blocked):
        return "blocked"
    if _contains_any_phrase(lowered, explicit_in_review):
        return "in_review"
    if _contains_any_phrase(lowered, explicit_in_progress):
        return "in_progress"
    if _contains_any_phrase(lowered, explicit_done):
        return "done"
    return "normal"


@app.route(route="health", methods=["GET"])
def health(req: func.HttpRequest) -> func.HttpResponse:
    logger.info("health called")
    return func.HttpResponse("Function App is working", status_code=200)


def _ensure_warm() -> None:
    global _WARMED_UP
    logger.info("_ensure_warm called: warmed_up=%s", _WARMED_UP)
    if _WARMED_UP:
        return
    with _WARMUP_LOCK:
        if _WARMED_UP:
            return
        started_at = time.time()
        logger.warning("Warmup starting")
        _get_store()
        _get_orchestrator()
        _WARMED_UP = True
        logger.warning("Warmup completed duration_seconds=%.3f", time.time() - started_at)


@app.route(route="standup/start", methods=["POST"])
def standup_start(req: func.HttpRequest) -> func.HttpResponse:
    logger.info("standup_start called")
    _ensure_warm()
    _ensure_monitor_started()
    try:
        body = req.get_json()
    except ValueError:
        logger.warning("standup_start received invalid JSON")
        return func.HttpResponse("Invalid JSON", status_code=400)

    project_key = body.get("project_key")
    meeting_url = body.get("meeting_url")
    if not meeting_url:
        logger.warning("standup_start missing meeting_url")
        return func.HttpResponse(
            json.dumps({"error": "meeting_url is required"}),
            mimetype="application/json",
            status_code=400,
        )

    logger.info(
        "standup_start payload parsed: project_key=%s meeting_url_present=%s",
        project_key,
        bool(meeting_url),
    )
    orch = _get_orchestrator()
    state = orch.start_standup(project_key=project_key, meeting_url=meeting_url)
    logger.info(
        "standup_start completed: instance_id=%s bot_id=%s issues_count=%s",
        state.get("instance_id"),
        state.get("bot_id"),
        len(state.get("issues", []) or []),
    )
    return func.HttpResponse(json.dumps(state), mimetype="application/json", status_code=200)


@app.route(route="standup/state/{instance_id}", methods=["GET"])
def standup_state(req: func.HttpRequest) -> func.HttpResponse:
    instance_id = req.route_params.get("instance_id", "")
    logger.info("standup_state called: instance_id=%s", instance_id)
    store = _get_store()
    state = store.get_state(instance_id)
    return func.HttpResponse(json.dumps(state), mimetype="application/json", status_code=200)


@app.route(route="recall/webhook", methods=["POST"])
def recall_webhook(req: func.HttpRequest) -> func.HttpResponse:
    logger.info("recall_webhook called")
    _ensure_monitor_started()
    try:
        body: Dict[str, Any] = req.get_json()
    except ValueError:
        logger.warning("recall_webhook received invalid JSON")
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON"}),
            mimetype="application/json",
            status_code=400,
        )

    logger.info("Recall webhook event received=%s", body)

    event_name = str(body.get("event") or body.get("event_type") or "")
    outer_data = body.get("data", {}) or {}
    inner_data = outer_data.get("data", {}) or {}

    logger.info("Webhook event=%s", event_name)
    logger.info(
        "Webhook outer_data keys=%s",
        list(outer_data.keys()) if isinstance(outer_data, dict) else [],
    )
    logger.info(
        "Webhook inner_data keys=%s",
        list(inner_data.keys()) if isinstance(inner_data, dict) else [],
    )
    logger.info("Webhook bot object=%s", outer_data.get("bot"))

    bot_id = body.get("bot_id") or outer_data.get("bot", {}).get("id")
    logger.info("Extracted bot_id=%s", bot_id)

    if not bot_id:
        logger.warning("bot_id missing — aborting event=%s", event_name)
        return func.HttpResponse(
            json.dumps({"status": "ignored", "reason": "missing_bot_id", "event": event_name}),
            mimetype="application/json",
            status_code=200,
        )

    if event_name in {"participant_events.speech_on", "participant_events.speech_off"}:
        return _handle_speech_event(event_name=event_name, bot_id=bot_id, body=body)

    store = _get_store()
    participant = inner_data.get("participant", {}) if isinstance(inner_data, dict) else {}
    transcript_speaker_name = str(participant.get("name") or "Unknown")

    # Clear stale bot-speaking state as soon as human transcript traffic arrives.
    if transcript_speaker_name.strip().lower() != BOT_SPEAKER_NAME:
        instance_id = store.get_instance_id_by_bot_id(bot_id)
        logger.info("Transcript mapping: bot_id=%s -> instance_id=%s", bot_id, instance_id)
        if instance_id:
            state = store.get_state(instance_id)
            if state.get("is_bot_speaking"):
                logger.warning(
                    "Clearing stale bot speaking flag from transcript traffic: instance_id=%s speaker_name=%s event=%s",
                    instance_id,
                    transcript_speaker_name,
                    event_name,
                )
                store.set_bot_speaking(instance_id, False, ends_at=time.time())

    transcript = _extract_transcript_payload(data=outer_data, body=body)
    if not transcript:
        logger.warning("No transcript payload extracted for event=%s", event_name)
        return func.HttpResponse(
            json.dumps({"status": "ignored", "reason": "no_transcript_payload", "event": event_name}),
            mimetype="application/json",
            status_code=200,
        )

    speaker_name = str(transcript.get("speaker_name") or transcript.get("speaker") or "Unknown")
    text = str(transcript.get("text") or transcript.get("transcript") or "").strip()
    is_final = bool(transcript.get("is_final", event_name == "transcript.data"))
    logger.info(
        "Transcript extracted: speaker_name=%s is_final=%s text=%s",
        speaker_name,
        is_final,
        text,
    )

    if not text and event_name == "transcript.partial_data":
        logger.info("Ignoring empty partial transcript event=%s", event_name)
        return func.HttpResponse(
            json.dumps({"status": "ignored", "reason": "empty_partial", "event": event_name}),
            mimetype="application/json",
            status_code=200,
        )

    if speaker_name.strip().lower() == BOT_SPEAKER_NAME:
        logger.info("Ignoring bot transcript for event=%s", event_name)
        return func.HttpResponse(
            json.dumps({"status": "ignored", "reason": "bot_speaker", "event": event_name}),
            mimetype="application/json",
            status_code=200,
        )

    instance_id = store.get_instance_id_by_bot_id(bot_id)
    logger.info("Webhook mapping: bot_id=%s -> instance_id=%s", bot_id, instance_id)
    if not instance_id:
        logger.warning("bot mapping not found for bot_id=%s", bot_id)
        return func.HttpResponse(
            json.dumps({"status": "ignored", "reason": "bot_mapping_not_found", "bot_id": bot_id}),
            mimetype="application/json",
            status_code=200,
        )

    store = _get_store()
    state = store.get_state(instance_id)
    reply_state = state.get("reply_state", {})
    logger.info(
        "State before transcript buffering: instance_id=%s is_bot_speaking=%s pending_bot_completion=%s status=%s",
        instance_id,
        state.get("is_bot_speaking"),
        reply_state.get("pending_bot_completion"),
        state.get("status"),
    )
    if state.get("is_bot_speaking") or reply_state.get("pending_bot_completion"):
        logger.warning(
            "Ignoring transcript because bot is still speaking or reply window not open: instance_id=%s event=%s",
            instance_id,
            event_name,
        )
        return func.HttpResponse(
            json.dumps(
                {
                    "status": "ignored",
                    "reason": "bot_still_speaking",
                    "instance_id": instance_id,
                    "event": event_name,
                }
            ),
            mimetype="application/json",
            status_code=200,
        )

    buffer_intent = str(reply_state.get("intent") or "normal")
    state = store.buffer_transcript(
        instance_id=instance_id,
        speaker_name=speaker_name,
        text=text,
        is_final=is_final,
        event_name=event_name,
        intent=buffer_intent,
        force_finalize_after_seconds=None,
    )
    merged_reply_state = state.get("reply_state") or {}
    merged_text = str(
        merged_reply_state.get("combined_text")
        or merged_reply_state.get("partial_text")
        or text
        or ""
    ).strip()
    logger.info(
        "Transcript buffered before intent detection: instance_id=%s speaker_name=%s raw_text=%s merged_text=%s is_final=%s",
        instance_id,
        speaker_name,
        text,
        merged_text,
        is_final,
    )

    if event_name == "transcript.partial_data" and not is_final:
        logger.info(
            "Buffered partial transcript only; skipping intent detection until final/speech_off: instance_id=%s merged_len=%s",
            instance_id,
            len(merged_text),
        )
        return func.HttpResponse(
            json.dumps(
                {
                    "status": "buffered_partial",
                    "instance_id": instance_id,
                    "speaker_name": speaker_name,
                    "text": text,
                    "merged_text": merged_text,
                    "event": event_name,
                    "is_final": is_final,
                    "reply_state": merged_reply_state,
                }
            ),
            mimetype="application/json",
            status_code=200,
        )

    if len(merged_text) < MIN_TRANSCRIPT_LENGTH and event_name == "transcript.data":
        logger.info(
            "Ignoring short final transcript after merge: event=%s merged_text_len=%s",
            event_name,
            len(merged_text),
        )
        return func.HttpResponse(
            json.dumps({"status": "ignored", "reason": "too_short", "event": event_name, "merged_text": merged_text}),
            mimetype="application/json",
            status_code=200,
        )

    logging.info("Passing merged text to intent=%s", merged_text)
    intent = detect_intent(merged_text)
    logging.info("Intent=%s", intent)
    force_finalize_after_seconds = EARLY_FINALIZE_SECONDS if intent in {"end", "skip", "blocked", "done", "in_review", "in_progress"} else None

    state = store.buffer_transcript(
        instance_id=instance_id,
        speaker_name=speaker_name,
        text="",
        is_final=False,
        event_name=f"{event_name}:intent",
        intent=intent,
        force_finalize_after_seconds=force_finalize_after_seconds,
    )
    logger.info(
        "Transcript intent updated: instance_id=%s speaker_name=%s intent=%s awaiting_reply=%s",
        instance_id,
        speaker_name,
        intent,
        (state.get("reply_state") or {}).get("awaiting_reply"),
    )

    return func.HttpResponse(
        json.dumps(
            {
                "status": "buffered",
                "instance_id": instance_id,
                "speaker_name": speaker_name,
                "text": text,
                "merged_text": merged_text,
                "event": event_name,
                "is_final": is_final,
                "intent": intent,
                "reply_state": state.get("reply_state", {}),
            }
        ),
        mimetype="application/json",
        status_code=200,
    )


def _extract_transcript_payload(data: Dict[str, Any], body: Dict[str, Any]) -> Dict[str, Any]:
    logger.info("_extract_transcript_payload called")
    inner_data = data.get("data") if isinstance(data.get("data"), dict) else {}
    participant = inner_data.get("participant") if isinstance(inner_data.get("participant"), dict) else {}
    words = inner_data.get("words") if isinstance(inner_data.get("words"), list) else []
    joined_words = " ".join(str(item.get("text", "")).strip() for item in words if item.get("text"))

    if joined_words:
        logger.info("Transcript words joined successfully: speaker_name=%s words=%s", participant.get("name"), joined_words)
        return {
            "speaker_name": participant.get("name") or "Unknown",
            "speaker": participant.get("name") or "Unknown",
            "text": joined_words.strip(),
            "transcript": joined_words.strip(),
            "is_final": body.get("event") == "transcript.data",
        }

    candidates = [
        data,
        body,
        data.get("transcript") if isinstance(data.get("transcript"), dict) else {},
        data.get("segment") if isinstance(data.get("segment"), dict) else {},
        body.get("transcript") if isinstance(body.get("transcript"), dict) else {},
        body.get("segment") if isinstance(body.get("segment"), dict) else {},
    ]

    for candidate in candidates:
        if not candidate:
            continue
        text = candidate.get("text") or candidate.get("transcript")
        if text:
            logger.info("Transcript extracted from fallback candidate")
            return candidate
    logger.warning("No transcript payload found in any candidate")
    return {}


def detect_intent(text: str) -> str:
    logger.info("detect_intent called: text_len=%s", len(text or ""))
    lowered = (text or "").lower()
    if any(keyword in lowered for keyword in SKIP_KEYWORDS):
        return "skip"
    explicit_status_intent = _detect_explicit_status_intent(text)
    if explicit_status_intent != "normal":
        return explicit_status_intent
    if any(keyword in lowered for keyword in END_KEYWORDS):
        return "end"
    return "normal"


def _handle_speech_event(event_name: str, bot_id: str, body: Dict[str, Any]) -> func.HttpResponse:
    logger.info("_handle_speech_event called: event_name=%s bot_id=%s", event_name, bot_id)
    store = _get_store()
    instance_id = store.get_instance_id_by_bot_id(bot_id)
    logger.info("Speech event mapping: bot_id=%s -> instance_id=%s", bot_id, instance_id)
    if not instance_id:
        logger.warning("Speech event bot mapping not found for bot_id=%s", bot_id)
        return func.HttpResponse(
            json.dumps({"status": "ignored", "reason": "bot_mapping_not_found", "bot_id": bot_id}),
            mimetype="application/json",
            status_code=200,
        )

    outer_data = body.get("data", {}) or {}
    inner_data = outer_data.get("data", {}) or {}
    participant = inner_data.get("participant", {}) if isinstance(inner_data, dict) else {}
    speaker_name = str(participant.get("name") or "Unknown")
    speaker_is_bot = speaker_name.strip().lower() == BOT_SPEAKER_NAME
    logger.info(
        "Speech event participant resolved: instance_id=%s speaker_name=%s speaker_is_bot=%s",
        instance_id,
        speaker_name,
        speaker_is_bot,
    )

    if speaker_is_bot:
        if event_name == "participant_events.speech_on":
            logger.info("Bot speech started: instance_id=%s", instance_id)
            store.set_bot_speaking(instance_id, True, started_at=time.time())
        else:
            logger.info("Bot speech ended: instance_id=%s", instance_id)
            state = store.set_bot_speaking(instance_id, False, ends_at=time.time())
            reply_state = state.get("reply_state", {})
            if reply_state.get("pending_bot_completion"):
                logger.info("Activating reply window after bot speech completion: instance_id=%s", instance_id)
                store.activate_ready_reply_windows(now=time.time())
        return func.HttpResponse(
            json.dumps(
                {
                    "status": "tracked",
                    "event": event_name,
                    "instance_id": instance_id,
                    "speaker_name": speaker_name,
                }
            ),
            mimetype="application/json",
            status_code=200,
        )

    if event_name == "participant_events.speech_on":
        current_state = store.get_state(instance_id)
        if current_state.get("is_bot_speaking"):
            logger.warning(
                "Clearing stale bot speaking flag from human speech_on: instance_id=%s speaker_name=%s",
                instance_id,
                speaker_name,
            )
            store.set_bot_speaking(instance_id, False, ends_at=time.time())
        logger.info("Tracking human speech_on: instance_id=%s speaker_name=%s", instance_id, speaker_name)
        store.buffer_transcript(
            instance_id=instance_id,
            speaker_name=speaker_name,
            text="",
            is_final=False,
            event_name=event_name,
            intent="normal",
        )
    else:
        reply_state = store.get_reply_state(instance_id)
        if reply_state.get("awaiting_reply"):
            logger.info("Scheduling finalize after human speech_off: instance_id=%s speaker_name=%s", instance_id, speaker_name)
            state = store.get_state(instance_id)
            reply_state["finalize_after_ts"] = time.time() + EARLY_FINALIZE_SECONDS
            state["reply_state"] = reply_state
            store.save_state(instance_id, state)
        else:
            logger.info("Ignoring human speech_off because reply window is not active: instance_id=%s", instance_id)

    return func.HttpResponse(
        json.dumps(
            {
                "status": "tracked",
                "event": event_name,
                "instance_id": instance_id,
                "speaker_name": speaker_name,
            }
        ),
        mimetype="application/json",
        status_code=200,
    )


def _ensure_monitor_started() -> None:
    global _MONITOR_STARTED
    logger.info("_ensure_monitor_started called: monitor_started=%s", _MONITOR_STARTED)
    if _MONITOR_STARTED:
        return
    thread = threading.Thread(target=_reply_monitor_loop, name="standup-reply-monitor", daemon=True)
    thread.start()
    _MONITOR_STARTED = True
    logger.info("Standup reply monitor thread started")


def _reply_monitor_loop() -> None:
    logger.info(
        "Standup reply monitor started: silence_timeout=%s no_response_timeout=%s early_finalize=%s",
        SILENCE_TIMEOUT_SECONDS,
        NO_RESPONSE_TIMEOUT_SECONDS,
        EARLY_FINALIZE_SECONDS,
    )
    while True:
        try:
            now = time.time()
            _ensure_warm()
            store = _get_store()
            orch = _get_orchestrator()
            activated = store.activate_ready_reply_windows(now=now)
            if activated:
                logger.info("Activated reply windows for instances=%s", activated)
            for item in store.list_active_reply_windows():
                instance_id = item["instance_id"]
                reply_state = item.get("reply_state", {})
                combined_text = str(reply_state.get("combined_text") or "").strip()
                last_activity_ts = float(reply_state.get("last_activity_ts") or now)
                reply_window_started_ts = float(
                    reply_state.get("reply_window_opened_at_ts")
                    or reply_state.get("reply_window_started_ts")
                    or now
                )
                finalize_after_ts = reply_state.get("finalize_after_ts")

                should_finalize = False
                if finalize_after_ts is not None and now >= float(finalize_after_ts):
                    should_finalize = True
                elif combined_text and now - last_activity_ts >= SILENCE_TIMEOUT_SECONDS:
                    should_finalize = True
                elif not combined_text and now - reply_window_started_ts >= NO_RESPONSE_TIMEOUT_SECONDS:
                    logger.warning("No response timeout reached for instance_id=%s", instance_id)
                    orch.save_reply_and_advance(
                        instance_id=instance_id,
                        speaker_name="No response",
                        text="No update received.",
                        intent="skip",
                    )
                    continue

                if should_finalize:
                    logger.info("Finalizing buffered reply for instance_id=%s", instance_id)
                    orch.finalize_buffered_reply(instance_id)
        except Exception:
            logger.exception("Standup reply monitor loop error")
        time.sleep(0.5)


def _get_orchestrator() -> Orchestrator:
    global _orch
    if _orch is None:
        logger.info("Creating Orchestrator singleton")
        _orch = Orchestrator()
    return _orch


def _get_store() -> StateStore:
    global _store
    if _store is None:
        logger.info("Creating StateStore singleton")
        _store = StateStore()
    return _store
