import base64
import logging
import os
import time
from typing import Any, Dict, Optional

import requests


class RecallClient:
    def __init__(self) -> None:
        self.api_key = os.getenv("RECALL_API_KEY")
        self.base_url = os.getenv("RECALL_BASE_URL").rstrip("/")
        self.bot_name = os.getenv("RECALL_BOT_NAME")
        self.webhook_url = os.getenv("RECALL_WEBHOOK_URL").strip()


    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": self.api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def create_bot(
        self,
        meeting_url: str,
        dashboard_url: Optional[str] = None,
        webhook_url: Optional[str] = None,
        instance_id: str = None
    ) -> Dict[str, Any]:
        effective_webhook_url = (webhook_url or self.webhook_url or "").strip()
        dashboard_url_with_id = None
        if dashboard_url:
            sep = "&" if "?" in dashboard_url else "?"
            dashboard_url_with_id = f"{dashboard_url}{sep}instance_id={instance_id}" if instance_id else dashboard_url

        # payload = {
        #     "meeting_url": meeting_url,
        #     "webhook_url": effective_webhook_url


        # }
        logging.info("Create bot called")
        payload = {
            "meeting_url": meeting_url,
            "bot_name": self.bot_name,
            "recording_config": {
                "transcript": {
                    "provider": {
                        "recallai_streaming": {
                            "mode": "prioritize_low_latency",
                            "language_code": "en"
                        }
                    }
                },
                "realtime_endpoints": [
                    {
                        "type": "webhook",
                        "url": effective_webhook_url,
                        "events": [
                            "transcript.data",
                            "transcript.partial_data",
                            "participant_events.speech_on",
                            "participant_events.speech_off"
                        ]
                    }
                ]
            }
        }
        if dashboard_url and dashboard_url_with_id:
            payload["output_media"] = {
                "camera": {
                    "kind": "webpage",
                    "config": {"url": dashboard_url_with_id},
                }
            }
        logging.info("create bot payload=%s",payload)
        
        response = requests.post(
            f"{self.base_url}/api/v1/bot/",
            headers=self._headers(),
            json=payload,
            timeout=30,
        )
        logging.info("Recall create_bot status_code=%s", response.status_code)
        response.raise_for_status()
        return response.json()

    def get_bot(self, bot_id: str) -> Dict[str, Any]:
        response = requests.get(
            f"{self.base_url}/api/v1/bot/{bot_id}/",
            headers=self._headers(),
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def wait_until_joined(self, bot_id: str, timeout_seconds: int = 180) -> Dict[str, Any]:
        deadline = time.time() + timeout_seconds
        last: Dict[str, Any] = {}

        while time.time() < deadline:
            last = self.get_bot(bot_id)
            status_changes = last.get("status_changes", []) or []
            latest = status_changes[-1] if status_changes else {}
            code = str(latest.get("code", "")).lower()

            if code in {"in_call", "joined", "recording", "in_call_recording", "in_call_not_recording"}:
                return last

            if code == "in_waiting_room":
                logging.warning("Bot is in waiting room: bot_id=%s", bot_id)
            time.sleep(3)

        raise TimeoutError(f"Bot {bot_id} did not join within {timeout_seconds} seconds")

    def start_webpage_output(self, bot_id: str, dashboard_url: str, instance_id: str) -> Dict[str, Any]:
        sep = "&" if "?" in dashboard_url else "?"
        dashboard_url_with_id = f"{dashboard_url}{sep}instance_id={instance_id}"
        payload = {
            "camera": {
                "kind": "webpage",
                "config": {"url": dashboard_url_with_id},
            }
        }
        logging.info("start webpage output called payload=%s",payload)
        response = requests.post(
            f"{self.base_url}/api/v1/bot/{bot_id}/output_media/",
            headers=self._headers(),
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def send_audio_mp3(self, bot_id: str, mp3_bytes: bytes) -> Dict[str, Any]:
        if not mp3_bytes:
            raise ValueError("mp3_bytes is empty")

        payload = {
            "kind": "mp3",
            "b64_data": base64.b64encode(mp3_bytes).decode("utf-8"),
        }
        response = requests.post(
            f"{self.base_url}/api/v1/bot/{bot_id}/output_audio/",
            headers=self._headers(),
            json=payload,
            timeout=60,
        )
        response.raise_for_status()
        return response.json() if response.text else {"ok": True}
