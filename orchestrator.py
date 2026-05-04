import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional

from agent_client import AgentClient
from azure_speech import AzureSpeechClient
from recall_client import RecallClient
from state_store import StateStore

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self) -> None:
        logger.info("Orchestrator.__init__ called")
        self.store = StateStore()
        self.agent = AgentClient()
        self.recall = RecallClient()
        self.speech = AzureSpeechClient()
        self.dashboard_url = os.getenv("DASHBOARD_URL", "")
        self.recall_webhook_url = os.getenv("RECALL_WEBHOOK_URL", "")
        self.advance_cooldown_seconds = float(os.getenv("ADVANCE_COOLDOWN_SECONDS", "3"))
        self.dashboard_display_delay_seconds = float(os.getenv("DASHBOARD_DISPLAY_DELAY_SECONDS", "15"))
        self.greeting_leading_silence_ms = int(os.getenv("GREETING_LEADING_SILENCE_MS", "1200"))
        logger.info(
            "Orchestrator initialized: dashboard_url_present=%s webhook_url_present=%s dashboard_display_delay_seconds=%s greeting_leading_silence_ms=%s",
            bool(self.dashboard_url),
            bool(self.recall_webhook_url),
            self.dashboard_display_delay_seconds,
            self.greeting_leading_silence_ms,
        )

    def start_standup(self, project_key: str, meeting_url: str) -> Dict[str, Any]:
        logger.info("start_standup called: project_key=%s meeting_url_present=%s", project_key, bool(meeting_url))
        try:
            self.store.client.ping()
            redis_status = "connected"
        except Exception:
            redis_status = "FAILED"
        logger.info("start_standup redis_status=%s", redis_status)

        instance_id = str(uuid.uuid4())
        logger.info("Generated standup instance_id=%s", instance_id)

        jiras = self.agent.get_active_jiras(project_key=project_key)
        standup_jiras = self._build_standup_jira_queue(jiras)
        logger.info(
            "Retrieved project jiras: total_count=%s standup_queue_count=%s project_key=%s",
            len(jiras),
            len(standup_jiras),
            project_key,
        )

        created = self.recall.create_bot(
            meeting_url=meeting_url,
            dashboard_url=self.dashboard_url or None,
            webhook_url=self.recall_webhook_url or None,
            instance_id=instance_id,
        )
        bot_id = created["id"]
        logger.info("Recall bot created: bot_id=%s", bot_id)
        self.store.map_bot_to_instance(bot_id=bot_id, instance_id=instance_id)
        logger.info("Mapped bot_id to instance_id: bot_id=%s instance_id=%s", bot_id, instance_id)

        self.recall.wait_until_joined(bot_id)
        logger.info("Recall bot joined meeting: bot_id=%s", bot_id)

        if self.dashboard_url:
            logger.info("Starting dashboard webpage output: bot_id=%s dashboard_url=%s instance_id=%s", bot_id, self.dashboard_url, instance_id)
            self.recall.start_webpage_output(bot_id, self.dashboard_url, instance_id)

        current_jira = standup_jiras[0] if standup_jiras else None
        state: Dict[str, Any] = {
            "instance_id": instance_id,
            "project_key": project_key,
            "meeting_url": meeting_url,
            "bot_id": bot_id,
            "jiras": jiras,
            "standup_jiras": standup_jiras,
            "current_index": 0,
            "current_jira": current_jira,
            "completed_jiras": [],
            "replies": [],
            "spoken_events": [],
            "status": "waiting_for_update" if current_jira else "completed",
            "dashboard_jira_count": len(jiras),
            "standup_jira_count": len(standup_jiras),
            "reply_state": {},
            "is_bot_speaking": False,
            "last_advance_ts": 0.0,
            "bot_speaking_started_ts": None,
            "bot_speaking_ends_at_ts": None,
        }

        self.store.save_state(instance_id, state)
        logger.info(
            "Initial standup state saved: instance_id=%s current_jira=%s jiras_count=%s",
            instance_id,
            (current_jira or {}).get("key") if current_jira else None,
            len(jiras),
        )

        greeting = self.agent.create_support_greeting(
            team_name=os.getenv("STANDUP_TEAM_NAME"),
            include_dashboard_notice=bool(self.dashboard_url),
            include_mute_reminder=False,
        )
        logger.info("Sending standup greeting before jira review: instance_id=%s", instance_id)
        state["status"] = "initializing"
        self.store.save_state(instance_id, state)
        state = self._speak_and_record(instance_id=instance_id, state=state, text=greeting, stage="greeting")

        if self.dashboard_display_delay_seconds > 0:
            logger.info(
                "Waiting after greeting for dashboard camera to display: bot_id=%s delay_seconds=%s",
                bot_id,
                self.dashboard_display_delay_seconds,
            )
            time.sleep(self.dashboard_display_delay_seconds)

        if current_jira:
            intro = self.agent.create_jira_intro(current_jira, position=1, total=len(standup_jiras))
            state = self._speak_and_record(instance_id=instance_id, state=state, text=intro, stage="jira_intro")
        else:
            logger.warning("No active jiras returned; sending closing message immediately")
            closing = self.agent.create_closing_text(processed_count=0)
            state = self._speak_and_record(instance_id=instance_id, state=state, text=closing, stage="closing")

        return state

    def save_reply(
        self,
        instance_id: str,
        speaker_name: str,
        text: str,
        intent: str = "normal",
    ) -> Dict[str, Any]:
        logger.info(
            "save_reply called: instance_id=%s speaker_name=%s intent=%s text_len=%s",
            instance_id,
            speaker_name,
            intent,
            len(text or ""),
        )
        state = self.store.get_state(instance_id)
        if not state:
            logger.warning("save_reply missing state for instance_id=%s", instance_id)
            raise ValueError(f"Unknown standup instance_id: {instance_id}")

        current_jira = state.get("current_jira") or {}
        reply = {
            "jira_key": current_jira.get("key"),
            "speaker_name": speaker_name,
            "text": text,
            "intent": intent,
        }
        transition_result = self._maybe_update_jira_status(current_jira=current_jira, text=text, intent=intent)
        if transition_result:
            reply["jira_transition"] = transition_result
            if transition_result.get("ok") and transition_result.get("new_status"):
                new_status = transition_result["new_status"]
                current_jira["status"] = new_status
                for jira in state.get("jiras", []):
                    if jira.get("key") == current_jira.get("key"):
                        jira["status"] = new_status
                for jira in state.get("standup_jiras", []):
                    if jira.get("key") == current_jira.get("key"):
                        jira["status"] = new_status
                state["current_jira"] = current_jira
                logger.info("Updated current jira status from reply: jira_key=%s new_status=%s", current_jira.get("key"), new_status)
            elif not transition_result.get("ok"):
                logger.warning(
                    "Jira transition failed for jira_key=%s message=%s",
                    current_jira.get("key"),
                    transition_result.get("message"),
                )

        replies = state.get("replies", [])
        replies.append(reply)
        state["replies"] = replies
        state["last_reply"] = reply
        state["status"] = "reply_received"
        self.store.save_state(instance_id, state)
        self.store.clear_reply_window(instance_id)
        logger.info("Reply saved: instance_id=%s jira_key=%s total_replies=%s", instance_id, current_jira.get("key"), len(replies))
        return state

    def advance(self, instance_id: str) -> Dict[str, Any]:
        logger.info("advance called: instance_id=%s", instance_id)
        state = self.store.get_state(instance_id)
        if not state:
            logger.warning("advance missing state for instance_id=%s", instance_id)
            raise ValueError(f"Unknown standup instance_id: {instance_id}")

        jiras: List[Dict[str, Any]] = state.get("jiras", [])
        standup_jiras: List[Dict[str, Any]] = state.get("standup_jiras", [])
        current_index = int(state.get("current_index", 0))
        current_jira = state.get("current_jira")

        if current_jira:
            completed = state.get("completed_jiras", [])
            if not any(item.get("key") == current_jira.get("key") for item in completed):
                completed.append(current_jira)
                state["completed_jiras"] = completed
                logger.info("Marked jira completed in standup flow: instance_id=%s jira_key=%s", instance_id, current_jira.get("key"))

        next_index = current_index + 1
        if next_index >= len(standup_jiras):
            state["current_index"] = len(standup_jiras)
            state["current_jira"] = None
            state["status"] = "completed"
            self.store.save_state(instance_id, state)
            logger.info("advance reached end of jira list: instance_id=%s completed_count=%s", instance_id, len(state.get("completed_jiras", [])))
            closing = self.agent.create_closing_text(processed_count=len(state.get("completed_jiras", [])))
            state = self._speak_and_record(instance_id=instance_id, state=state, text=closing, stage="closing")
            return state

        next_jira = standup_jiras[next_index]
        state["current_index"] = next_index
        state["current_jira"] = next_jira
        state["status"] = "waiting_for_update"
        self.store.save_state(instance_id, state)
        logger.info("Advanced to next jira: instance_id=%s next_index=%s jira_key=%s", instance_id, next_index, next_jira.get("key"))

        transition = self.agent.create_transition_text(
            next_jira,
            position=next_index + 1,
            total=len(standup_jiras),
        )
        state = self._speak_and_record(instance_id=instance_id, state=state, text=transition, stage="jira_intro")
        return state

    def _mark_advance_if_allowed(self, instance_id: str) -> Optional[Dict[str, Any]]:
        logger.info("_mark_advance_if_allowed called: instance_id=%s", instance_id)
        state = self.store.get_state(instance_id)
        if not state:
            logger.warning("_mark_advance_if_allowed missing state for instance_id=%s", instance_id)
            return None

        now = time.time()
        last_advance_ts = float(state.get("last_advance_ts") or 0.0)
        delta = now - last_advance_ts
        if delta < self.advance_cooldown_seconds:
            logger.warning(
                "Advance ignored due to cooldown: instance_id=%s delta_seconds=%.3f cooldown_seconds=%.3f",
                instance_id,
                delta,
                self.advance_cooldown_seconds,
            )
            return state

        state["last_advance_ts"] = now
        self.store.save_state(instance_id, state)
        logger.info(
            "Advance allowed and timestamp updated: instance_id=%s last_advance_ts=%s",
            instance_id,
            now,
        )
        return state

    def save_reply_and_advance(
        self,
        instance_id: str,
        speaker_name: str,
        text: str,
        intent: str = "normal",
    ) -> Dict[str, Any]:
        logger.info("save_reply_and_advance called: instance_id=%s speaker_name=%s intent=%s", instance_id, speaker_name, intent)
        pre_state = self.store.get_state(instance_id)
        if not pre_state:
            logger.warning("save_reply_and_advance missing state for instance_id=%s", instance_id)
            raise ValueError(f"Unknown standup instance_id: {instance_id}")

        now = time.time()
        last_advance_ts = float(pre_state.get("last_advance_ts") or 0.0)
        delta = now - last_advance_ts
        if delta < self.advance_cooldown_seconds:
            logger.warning(
                "Advance ignored due to cooldown: instance_id=%s delta_seconds=%.3f cooldown_seconds=%.3f intent=%s",
                instance_id,
                delta,
                self.advance_cooldown_seconds,
                intent,
            )
            return pre_state

        pre_state["last_advance_ts"] = now
        self.store.save_state(instance_id, pre_state)
        logger.info("Advance cooldown gate passed: instance_id=%s last_advance_ts=%s", instance_id, now)

        saved_state = self.save_reply(instance_id=instance_id, speaker_name=speaker_name, text=text, intent=intent)
        last_reply = saved_state.get("last_reply") or {}
        transition_result = last_reply.get("jira_transition") or {}
        current_index = int(saved_state.get("current_index", 0))
        standup_jiras = saved_state.get("standup_jiras", [])
        is_last_jira = current_index >= len(standup_jiras) - 1
        if transition_result.get("ok"):
            narration = self.agent.build_transition_narration(
                jira_key=str(last_reply.get("jira_key") or "").strip(),
                old_status=str(transition_result.get("old_status") or "").strip(),
                new_status=str(transition_result.get("new_status") or "").strip(),
                already_in_target=bool(transition_result.get("already_in_target")),
                is_last_jira=is_last_jira,
            )
            if narration:
                logger.info(
                    "Narrating Jira status change: instance_id=%s jira_key=%s narration=%s",
                    instance_id,
                    last_reply.get("jira_key"),
                    narration,
                )
                self._speak_and_record(instance_id=instance_id, state=saved_state, text=narration, stage="transition_update")
                transition_pause_seconds = max(1.5, min(6.0, len(narration.split()) / 2.2))
                logger.info(
                    "Waiting for transition narration playback before advance: instance_id=%s pause_seconds=%s",
                    instance_id,
                    transition_pause_seconds,
                )
                time.sleep(transition_pause_seconds)
        else:
            narration = self.agent.build_acknowledgement_narration(
                jira_key=str(last_reply.get("jira_key") or "").strip(),
                is_last_jira=is_last_jira,
            )
            if narration:
                logger.info(
                    "Narrating acknowledgement (no transition): instance_id=%s jira_key=%s narration=%s",
                    instance_id,
                    last_reply.get("jira_key"),
                    narration,
                )
                self._speak_and_record(instance_id=instance_id, state=saved_state, text=narration, stage="transition_update")
                transition_pause_seconds = max(1.5, min(6.0, len(narration.split()) / 2.2))
                logger.info(
                    "Waiting for acknowledgement playback before advance: instance_id=%s pause_seconds=%s",
                    instance_id,
                    transition_pause_seconds,
                )
                time.sleep(transition_pause_seconds)
        self.store.clear_reply_window(instance_id)
        time.sleep(1.5)
        return self.advance(instance_id=instance_id)
        

    def finalize_buffered_reply(self, instance_id: str) -> Optional[Dict[str, Any]]:
        logger.info("finalize_buffered_reply called: instance_id=%s", instance_id)
        state = self.store.get_state(instance_id)
        if not state:
            logger.warning("finalize_buffered_reply missing state for instance_id=%s", instance_id)
            return None
        reply_payload = self.store.consume_reply_buffer(instance_id)
        text = (reply_payload.get("text") or "").strip()
        if not text:
            logger.warning("finalize_buffered_reply found empty buffered text for instance_id=%s", instance_id)
            return None
        return self.save_reply_and_advance(
            instance_id=instance_id,
            speaker_name=str(reply_payload.get("speaker_name") or "Unknown"),
            text=text,
            intent=str(reply_payload.get("intent") or "normal"),
        )

    def get_state(self, instance_id: str) -> Dict[str, Any]:
        logger.info("get_state called: instance_id=%s", instance_id)
        return self.store.get_state(instance_id)


    @staticmethod
    def _build_standup_jira_queue(jiras: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        allowed_statuses = {"Blocked", "In Progress"}
        queue = [jira for jira in jiras if str(jira.get("status") or "") in allowed_statuses]
        logger.info("Built standup jira queue: total_jiras=%s queue_count=%s", len(jiras), len(queue))
        return queue

    def _speak_and_record(self, instance_id: str, state: Dict[str, Any], text: str, stage: str) -> Dict[str, Any]:
        logger.info("_speak_and_record called: instance_id=%s stage=%s text_len=%s", instance_id, stage, len(text or ""))
        leading_silence_ms = self.greeting_leading_silence_ms if stage == "greeting" else 0
        state["spoken_text"] = text
        spoken_events = state.get("spoken_events", [])
        spoken_events.append(
            {
                "stage": stage,
                "jira_key": (state.get("current_jira") or {}).get("key"),
                "text": text,
            }
        )
        state["spoken_events"] = spoken_events

        bot_id = state.get("bot_id")
        if not bot_id:
            logger.warning("_speak_and_record missing bot_id for instance_id=%s", instance_id)
            state["speech_error"] = "bot_id is missing"
            self.store.save_state(instance_id, state)
            return state

        try:
            audio = self.speech.synthesize_mp3(text, leading_silence_ms=leading_silence_ms)
            logger.info("Speech synthesized successfully: instance_id=%s stage=%s audio_bytes=%s", instance_id, stage, len(audio or b""))
            self.recall.send_audio_mp3(bot_id, audio)
            logger.info("Audio sent to Recall bot: instance_id=%s bot_id=%s stage=%s", instance_id, bot_id, stage)
            if stage not in {"closing", "transition_update"}:
                self.store.clear_reply_window(instance_id) 
                state = self.store.set_bot_speaking(instance_id, True, started_at=time.time())
                expected_duration_seconds = max(2.0, min(20.0, len(text.split()) / 2.2))
                open_at = time.time() + expected_duration_seconds
                logger.info(
                    "Scheduling reply window after narration: instance_id=%s expected_duration_seconds=%s open_at=%s",
                    instance_id,
                    expected_duration_seconds,
                    open_at,
                )
                state = self.store.schedule_reply_window(instance_id, open_at_ts=open_at, source_event="bot_prompt_sent")
            elif stage == "closing":
                state["status"] = "completed"
                self.store.save_state(instance_id, state)
            else:
                self.store.save_state(instance_id, state)
        except Exception as exc:
            logger.exception("Speech/send failed for instance_id=%s stage=%s", instance_id, stage)
            state["speech_error"] = str(exc)
            self.store.save_state(instance_id, state)
        return self.store.get_state(instance_id)


    def _maybe_update_jira_status(self, current_jira: Dict[str, Any], text: str, intent: str) -> Optional[Dict[str, Any]]:
        jira_key = str((current_jira or {}).get("key") or "").strip()
        logger.info("_maybe_update_jira_status called: jira_key=%s intent=%s text_len=%s", jira_key, intent, len(text or ""))
        if not jira_key:
            logger.warning("_maybe_update_jira_status skipped because jira key is missing")
            return None

        target_candidates = self._get_transition_candidates(text=text, intent=intent)
        if not target_candidates:
            logger.info("No transition candidates inferred for jira_key=%s", jira_key)
            return None

        current_status = str((current_jira or {}).get("status") or "").strip().lower()
        if any(current_status == candidate.lower() for candidate in target_candidates):
            logger.info("jira already in target status: jira_key=%s current_status=%s", jira_key, current_jira.get("status", ""))
            return {
                "ok": True,
                "jira_key": jira_key,
                "old_status": current_jira.get("status", ""),
                "transition_applied": current_jira.get("status", ""),
                "new_status": current_jira.get("status", ""),
                "message": "jira already in target status.",
                "already_in_target": True,
            }

        transitions = self.agent.get_jira_transitions(jira_key)
        if not transitions:
            logger.warning("No valid Jira transitions returned for jira_key=%s", jira_key)
            return {
                "ok": False,
                "jira_key": jira_key,
                "message": "No valid Jira transitions returned.",
            }

        selected = self._select_transition(transitions, target_candidates)
        if not selected:
            logger.warning("No matching Jira transition found for jira_key=%s candidates=%s", jira_key, target_candidates)
            return {
                "ok": False,
                "jira_key": jira_key,
                "message": f"No matching Jira transition found for candidates: {target_candidates}",
                "available_transitions": transitions,
            }

        result = self.agent.update_jira_status(
            jira_key,
            transition_id=selected.get("id") or None,
            transition_name=selected.get("name") or None,
        )
        result["target_candidates"] = target_candidates
        result["selected_transition"] = selected
        result["old_status"] = current_jira.get("status", "")
        result["new_status"] = result.get("new_status") or selected.get("to_status") or selected.get("name") or result.get("transition_applied") or ""
        logger.info(
            "Jira status update attempted: jira_key=%s selected_transition=%s ok=%s",
            jira_key,
            selected,
            result.get("ok"),
        )
        return result

    @staticmethod
    def _select_transition(transitions: List[Dict[str, Any]], target_candidates: List[str]) -> Optional[Dict[str, Any]]:
        logger.info("_select_transition called: transitions_count=%s candidates=%s", len(transitions), target_candidates)
        normalized_candidates = [item.lower() for item in target_candidates]
        for transition in transitions:
            name = str(transition.get("name") or "").strip().lower()
            to_status = str(transition.get("to_status") or transition.get("name") or "").strip().lower()
            if name in normalized_candidates or to_status in normalized_candidates:
                return transition
        for transition in transitions:
            name = str(transition.get("name") or "").strip().lower()
            to_status = str(transition.get("to_status") or transition.get("name") or "").strip().lower()
            if any(candidate in name or name in candidate or candidate in to_status or to_status in candidate for candidate in normalized_candidates):
                return transition
        return None

    @staticmethod
    def _get_transition_candidates(text: str, intent: str) -> List[str]:
        logger.info("_get_transition_candidates called: intent=%s text_len=%s", intent, len(text or ""))
        lowered = (text or "").lower()

        phrase_map = [
            ([
                "move to in review", "move to review", "mark in review", "mark as in review",
                "set to in review", "change to in review", "ready for review", "for review",
                "under review", "in review",
            ], ["In Review", "Review"]),
            ([
                "move to blocked", "move to block", "move this to blocked", "move this to block",
                "mark blocked", "mark as blocked", "mark this blocked", "set to blocked", "set blocked",
                "change to blocked", "put this on blocked", "put it on blocked",
                "this is blocked", "it is blocked", "blocked",
            ], ["Blocked"]),
            ([
                "move to in progress", "mark in progress", "mark as in progress",
                "set to in progress", "change to in progress", "keep in progress",
                "still in progress", "back to in progress", "in progress",
            ], ["In Progress"]),
            ([
                "move to done", "mark done", "mark as done", "set to done",
                "change to done", "this is done", "it is done", "move to resolved",
                "mark resolved", "move to closed", "mark closed", "done", "completed",
                "finished", "resolved", "closed",
            ], ["Done", "Resolved", "Closed"]),
            (["move to to do", "move to backlog", "mark to do", "mark backlog", "set to to do", "set to backlog"], ["To Do", "Backlog"]),
        ]

        if intent == "blocked":
            return ["Blocked"]
        if intent == "done":
            return ["Done", "Resolved", "Closed"]
        if intent == "in_review":
            return ["In Review", "Review"]
        if intent == "in_progress":
            return ["In Progress"]

        for phrases, candidates in phrase_map:
            if any(phrase in lowered for phrase in phrases):
                return candidates

        return []
