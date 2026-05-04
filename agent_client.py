import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
load_dotenv()

class AgentClient:
    def __init__(self) -> None:
        self.mode = os.getenv("AGENT_MODE", "foundry").lower()
        self.foundry_project_endpoint = os.getenv("FOUNDRY_PROJECT_ENDPOINT", "").rstrip("/")
        self.foundry_agent_name = os.getenv("FOUNDRY_AGENT_NAME", "")
        self.foundry_token = os.getenv("FOUNDRY_AGENT_TOKEN", "")
        self.default_project_key = os.getenv("JIRA_PROJECT_KEY", "KAN")
        self.max_jiras = int(os.getenv("STANDUP_MAX_jiraS", "25"))


    def get_active_jiras(self, project_key: str | None = None) -> List[Dict[str, Any]]:
        project_key = project_key or self.default_project_key
        logging.info(
            "Agent get_active_jiras called: project_key=%s endpoint=%s agent=%s mode=%s",
            project_key,
            self.foundry_project_endpoint,
            self.foundry_agent_name,
            self.mode,
        )

        if not self._is_foundry_configured():
            logging.warning("Agent client is not configured for foundry mode")
            return []

        prompt = (
            f"Use the Jira MCP function list_active_jiras for project {project_key}. "
            "Return only JSON as an array with at most "
            f"{self.max_jiras} objects. "
            "Each object must contain: key, summary, status, assignee, priority, description. "
            "Return all project jiras needed for dashboard visibility, but sort Blocked first, "
            "then In Progress, then In Review, then all remaining statuses."
        )
        logging.info("Agent get_active_jiras prompt=%s", prompt)

        data = self._call_foundry(prompt)
        logging.info("Data returned from foundry=%s",data)
        jiras = self._extract_json_array(data)
        jiras = self._sort_jiras_for_dashboard(jiras)
        logging.info("Agent returned %s project jiras for dashboard and standup queueing", len(jiras))
        return jiras

    def create_support_greeting(
        self,
        team_name: str | None = None,
        include_dashboard_notice: bool = True,
        include_mute_reminder: bool = False,
    ) -> str:
        team = team_name or "team"

        parts = [f"Good morning {team}. Welcome to our daily standup, we're starting now."]
        if include_dashboard_notice:
            parts.append("Please share your updates while we review the dashboard together.")
        if include_mute_reminder:
            parts.append("Please keep yourselves muted until called upon.")
        fallback = " ".join(parts)

        if not self._is_foundry_configured():
            return fallback

        context_parts = [f"The standup is for the {team}."]
        if include_dashboard_notice:
            context_parts.append("A dashboard will be screen shared showing all jiras.")
        if include_mute_reminder:
            context_parts.append("Participants should stay muted until called upon.")

        prompt = (
            "Generate a short, professional standup meeting greeting to be spoken aloud by a bot."
            "Team is in Singapore. Generate greeting timing according to that."
            "Return only plain speech text with no JSON, markdown, or formatting. "
            "Focus will be blocked jiras."
            "It should welcome the team, signal the standup is starting, and set expectations. "
            "Keep it under 3 sentences. "
            f"Context: {' '.join(context_parts)}"
        )
        try:
            data = self._call_foundry(prompt)
            narration = self._extract_plain_text(data).strip()
            if narration:
                logging.info("Agent greeting generated for team=%s", team)
                return narration
        except Exception:
            logging.exception("create_support_greeting agent call failed, using fallback")

        return fallback    

    def summarize_jira_for_standup(self, jira: Dict[str, Any]) -> str:
        if not jira:
            return "No summary provided"
        if not self._is_foundry_configured():
            return str(jira.get("summary") or "No summary provided")

        jira_json = json.dumps(
            {
                "key": jira.get("key"),
                "summary": jira.get("summary"),
                "status": jira.get("status"),
                "assignee": jira.get("assignee"),
                "priority": jira.get("priority"),
                "description": jira.get("description"),
            },
            ensure_ascii=False,
        )
        prompt = (
            "Use the Jira MCP function summarize_jira_for_standup on this jira object and "
            "return only the concise speech-friendly summary text, with no JSON or markdown. "
            f"jira: {jira_json}"
        )
        try:
            data = self._call_foundry(prompt)
            text = self._extract_plain_text(data).strip()
            return text or str(jira.get("summary") or "No summary provided")
        except Exception:
            logging.exception("Agent summarize_jira_for_standup failed for jira=%s", jira.get("key"))
            return str(jira.get("summary") or "No summary provided")


    def get_jira_transitions(self, jira_key: str) -> List[Dict[str, Any]]:
        if not jira_key or not self._is_foundry_configured():
            return []
        prompt = (
            f"Use the Jira MCP function get_jira_transitions for jira {jira_key}. "
            "Return only JSON as an array. Each item must contain id, name, and to_status."
        )
        data = self._call_foundry(prompt)
        transitions = []
        for item in self._extract_json_array(data):
            if not isinstance(item, dict):
                continue
            transitions.append({
                "id": str(item.get("id", "")).strip(),
                "name": str(item.get("name", "")).strip(),
                "to_status": str(item.get("to_status") or item.get("name") or "").strip(),
            })
        return [t for t in transitions if t["id"] or t["name"] or t["to_status"]]

    def update_jira_status(
        self,
        jira_key: str,
        *,
        transition_name: str | None = None,
        transition_id: str | None = None,
    ) -> Dict[str, Any]:
        if not jira_key:
            return {"ok": False, "reason": "missing_jira_key"}
        if not self._is_foundry_configured():
            return {"ok": False, "reason": "agent_not_configured"}
        if not transition_name and not transition_id:
            return {"ok": False, "reason": "missing_transition"}

        transition_desc = f"transition_id={transition_id}" if transition_id else f"transition_name={transition_name}"
        prompt = (
            f"Use the Jira MCP function update_jira_status for jira {jira_key} with {transition_desc}. "
            "Return only JSON with keys: ok, jira_key, transition_applied, message."
        )
        try:
            data = self._call_foundry(prompt)
            parsed = self._extract_first_json_object(data)
            if parsed:
                return {
                    "ok": bool(parsed.get("ok", True)),
                    "jira_key": str(parsed.get("jira_key") or jira_key),
                    "transition_applied": str(parsed.get("transition_applied") or transition_name or transition_id or ""),
                    "message": str(parsed.get("message") or ""),
                }
            text = self._extract_plain_text(data).strip()
            return {
                "ok": True,
                "jira_key": jira_key,
                "transition_applied": transition_name or transition_id or "",
                "message": text,
            }
        except Exception as exc:
            logging.exception("Agent update_jira_status failed for jira=%s", jira_key)
            return {"ok": False, "jira_key": jira_key, "transition_applied": transition_name or transition_id or "", "message": str(exc)}


    def create_jira_intro(self, jira: Dict[str, Any], position: int | None = None, total: int | None = None) -> str:
        assignee = jira.get("assignee") or "the team"
        jira_key = jira.get("key") or "Unknown jira"
        summary = self.summarize_jira_for_standup(jira)
        status = jira.get("status") or "Unknown"

        if status == "Blocked":
            status_phrase = "is currently blocked"
        else:
            status_phrase = f"is currently {status.lower()}"

        prompt = (
            "Generate a short, natural standup narration to be spoken aloud by a bot. "
            "Return only plain speech text with no JSON, markdown, or formatting. "
            "Keep it under 3 sentences. "
            "You must use all of the following details exactly as provided: "
            f"Jira summary: {summary}. "
            f"Status phrase: {status_phrase}. "
            f"Assignee: {assignee}. "
            "The narration must mention the summary, the status phrase, and ask the assignee for their update."
        )
        try:
            data = self._call_foundry(prompt)
            narration = self._extract_plain_text(data).strip()
            if narration:
                logging.info("Agent narration built for jira=%s", jira_key)
                return narration
        except Exception:
            logging.exception("create_jira_intro agent call failed for jira=%s, using fallback", jira_key)
        
        fallback = (
            f"Next jira: {summary}. "
            f"This item {status_phrase}. "
            f"{assignee}, please share your update."
        )
        logging.info("Agent narration built for jira=%s", jira_key)
        return fallback


    def create_transition_text(self, next_jira: Dict[str, Any], position: int, total: int) -> str:
        return self.create_jira_intro(next_jira, position=position, total=total)


    def create_closing_text(self, processed_count: int) -> str:
        if processed_count <= 0:
            fallback = "There are no blocked or in progress jiras to review today."
        else:
            fallback = f"That completes the standup review. We went through {processed_count} jiras."

        if self._is_foundry_configured():
            prompt = (
                "Generate a short, natural standup closing statement to be spoken aloud by a bot. "
                "Return only plain speech text with no JSON, markdown, or formatting. "
                "Keep it under 2 sentences. "
                f"You must use this detail exactly: {processed_count} jiras were reviewed. "
                + (
                    "There were no blocked or in progress jiras to review."
                    if processed_count <= 0
                    else f"The standup covered {processed_count} jiras in total."
                )
            )
            try:
                data = self._call_foundry(prompt)
                narration = self._extract_plain_text(data).strip()
                if narration:
                    logging.info("Agent closing text generated: processed_count=%s", processed_count)
                    return narration
            except Exception:
                logging.exception("create_closing_text agent call failed, using fallback")

        return fallback


    def build_transition_narration(
        self,
        jira_key: str,
        old_status: str,
        new_status: str,
        already_in_target: bool = False,
        is_last_jira: bool = False,
    ) -> str:
        jira_key = (jira_key or "").strip()
        old_status = (old_status or "").strip()
        new_status = (new_status or "").strip()

        if not jira_key:
            return ""

        if self._is_foundry_configured():
            if already_in_target:
                context = f"The jira {jira_key} was already in {new_status} status, no change was made."
            else:
                context = f"The jira {jira_key} status was changed from {old_status} to {new_status}."
            ending_instruction = (
                "Do not mention moving to another jira. End with a natural closing for the standup."
                if is_last_jira else
                "End by indicating the standup is moving on to the next jira."
            )
            prompt = (
                "Generate a short, natural standup status update narration to be spoken aloud by a bot. "
                "Return only plain speech text with no JSON, markdown, or formatting. "
                "Keep it to 1-2 sentences. "
                f"You can refer these details, and rephrase: {context} "
                f"{ending_instruction}"
            )
            try:
                data = self._call_foundry(prompt)
                narration = self._extract_plain_text(data).strip()
                if narration:
                    logging.info(
                        "Agent transition narration built: jira_key=%s old=%s new=%s is_last=%s",
                        jira_key, old_status, new_status, is_last_jira,
                    )
                    return narration
            except Exception:
                logging.exception(
                    "build_transition_narration agent call failed for jira_key=%s, using fallback",
                    jira_key,
                )
        ending = "That's all for today's standup. Thank you everyone." if is_last_jira else "Moving on to next jira."

        if already_in_target and new_status:
            fallback = f"{jira_key} remains in {new_status}. {ending}"
        elif old_status and new_status and old_status.lower() != new_status.lower():
            fallback = f"Got it. Moving {jira_key} from {old_status} to {new_status}. {ending}"
        elif new_status:
            fallback = f"Got it. Updating {jira_key} to {new_status}. {ending}"
        else:
            fallback =  f"No action taken. {ending}"
        return fallback


    def build_acknowledgement_narration(
        self,
        jira_key: str,
        is_last_jira: bool = False,
    ) -> str:
        jira_key = (jira_key or "").strip()
        if not jira_key:
            return ""


        if self._is_foundry_configured():
            ending_instruction = (
                "Do not mention moving to another jira. End with a natural closing for the standup."
                if is_last_jira else
                "End by indicating the standup is moving on to the next jira."
            )
            prompt = (
                "Generate a short, natural standup acknowledgement to be spoken aloud by a bot. "
                "Return only plain speech text with no JSON, markdown, or formatting. "
                "Keep it to 1 sentence. "
                f"The team member just gave their update for jira {jira_key}. Acknowledge their update briefly. "
                f"{ending_instruction}"
            )
            try:
                data = self._call_foundry(prompt)
                narration = self._extract_plain_text(data).strip()
                if narration:
                    logging.info("Agent acknowledgement narration built: jira_key=%s is_last=%s", jira_key, is_last_jira)
                    return narration
            except Exception:
                logging.exception("build_acknowledgement_narration agent call failed for jira_key=%s, using fallback", jira_key)
		
		ending = "That is all for today's standup, thank you everyone." if is_last_jira else "Moving on to the next jira."
        fallback = f"Thanks for the update on {jira_key}. {ending}"
        return fallback

    def _sort_jiras_for_dashboard(self, jiras: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        order = {"Blocked": 0, "In Progress": 1, "In Review": 2}
        sorted_jiras = sorted(
            jiras,
            key=lambda jira: (order.get(str(jira.get("status") or ""), 99), str(jira.get("priority") or ""), str(jira.get("key") or "")),
        )
        return sorted_jiras

    def _is_foundry_configured(self) -> bool:
        return (
            self.mode == "foundry"
            and bool(self.foundry_project_endpoint)
            and bool(self.foundry_agent_name)
            and bool(self.foundry_token)
        )

    def _call_foundry(self, prompt: str) -> Dict[str, Any]:
        url = self.foundry_project_endpoint
        response = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {self.foundry_token}",
                "Content-Type": "application/json",
            },
            json={
                "input": prompt,
            },
            timeout=60,
        )
        logging.info("Agent _call_foundry status_code=%s", response.status_code)
        response.raise_for_status()
        return response.json()

    def _extract_json_array(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        texts = self._collect_text_fragments(data)
        extracted_items: List[Dict[str, Any]] = []

        for text in texts:
            parsed = self._try_parse_json_array(text)
            if not parsed:
                continue
            for jira in parsed:
                if not isinstance(jira, dict):
                    continue
                normalized = {
                    "key": jira.get("key", "N/A"),
                    "status": jira.get("status", "Unknown"),
                    "assignee": jira.get("assignee", "Unassigned"),
                    "summary": jira.get("summary", "No Summary Provided"),
                    "priority": jira.get("priority", "Unknown"),
                    "description": jira.get("description", ""),
                    "id": jira.get("id", ""),
                    "name": jira.get("name", ""),
                    "to_status": jira.get("to_status", jira.get("name", "")),
                }
                extracted_items.append(normalized)
            if extracted_items:
                break

        if not extracted_items:
            logging.warning("No JSON jira array could be extracted from foundry response")
        return extracted_items

    def _extract_first_json_object(self, data: Dict[str, Any]) -> Dict[str, Any]:
        for text in self._collect_text_fragments(data):
            parsed = self._try_parse_json_object(text)
            if parsed:
                return parsed
        return {}

    def _extract_plain_text(self, data: Any) -> str:
        texts = self._collect_text_fragments(data)
        if texts:
            return "\n".join(texts)
        return ""

    def _collect_text_fragments(self, data: Any) -> List[str]:
        texts: List[str] = []

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                for key, value in node.items():
                    if key == "text" and isinstance(value, str):
                        texts.append(value)
                    else:
                        walk(value)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(data)
        return texts

    def _try_parse_json_array(self, text: str) -> List[Dict[str, Any]]:
        clean_text = self._strip_code_fence(text)

        candidates = [clean_text]
        array_match = re.search(r"(\[.*\])", clean_text, flags=re.DOTALL)
        if array_match:
            candidates.append(array_match.group(1))

        for candidate in candidates:
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, list):
                    return parsed
            except Exception:
                continue
        return []

    def _try_parse_json_object(self, text: str) -> Dict[str, Any]:
        clean_text = self._strip_code_fence(text)
        candidates = [clean_text]
        match = re.search(r"(\{.*\})", clean_text, flags=re.DOTALL)
        if match:
            candidates.append(match.group(1))
        for candidate in candidates:
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                continue
        return {}

    @staticmethod
    def _strip_code_fence(text: str) -> str:
        clean_text = text.strip()
        clean_text = re.sub(r"^```(?:json)?\s*", "", clean_text, flags=re.IGNORECASE)
        clean_text = re.sub(r"```$", "", clean_text).strip()
        return clean_text
