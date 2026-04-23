import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

import requests


class AgentClient:
    def __init__(self) -> None:
        self.mode = os.getenv("AGENT_MODE", "foundry").lower()
        self.foundry_project_endpoint = os.getenv("FOUNDRY_PROJECT_ENDPOINT", "").rstrip("/")
        self.foundry_agent_name = os.getenv("FOUNDRY_AGENT_NAME", "")
        self.foundry_token = os.getenv("FOUNDRY_AGENT_TOKEN", "")
        self.default_project_key = os.getenv("JIRA_PROJECT_KEY", "KAN")
        self.max_issues = int(os.getenv("STANDUP_MAX_ISSUES", "25"))

    def get_active_issues(self, project_key: str | None = None) -> List[Dict[str, Any]]:
        project_key = project_key or self.default_project_key
        logging.info(
            "Agent get_active_issues called: project_key=%s endpoint=%s agent=%s mode=%s",
            project_key,
            self.foundry_project_endpoint,
            self.foundry_agent_name,
            self.mode,
        )

        if not self._is_foundry_configured():
            logging.warning("Agent client is not configured for foundry mode")
            return []

        prompt = (
            f"Use the Jira MCP function list_active_issues for project {project_key}. "
            "Return only JSON as an array with at most "
            f"{self.max_issues} objects. "
            "Each object must contain: key, summary, status, assignee, priority, description. "
            "Return all project issues needed for dashboard visibility, but sort Blocked first, "
            "then In Progress, then In Review, then all remaining statuses."
        )
        logging.info("Agent get_active_issues prompt=%s", prompt)

        data = self._call_foundry(prompt)
        logging.info("Data returned from foundry=%s",data)
        issues = self._extract_json_array(data)
        issues = self._sort_issues_for_dashboard(issues)
        logging.info("Agent returned %s project issues for dashboard and standup queueing", len(issues))
        return issues

    def summarize_issue_for_standup(self, issue: Dict[str, Any]) -> str:
        if not issue:
            return "No summary provided"
        if not self._is_foundry_configured():
            return str(issue.get("summary") or "No summary provided")

        issue_json = json.dumps(
            {
                "key": issue.get("key"),
                "summary": issue.get("summary"),
                "status": issue.get("status"),
                "assignee": issue.get("assignee"),
                "priority": issue.get("priority"),
                "description": issue.get("description"),
            },
            ensure_ascii=False,
        )
        prompt = (
            "Use the Jira MCP function summarize_issue_for_standup on this issue object and "
            "return only the concise speech-friendly summary text, with no JSON or markdown. "
            f"Issue: {issue_json}"
        )
        try:
            data = self._call_foundry(prompt)
            text = self._extract_plain_text(data).strip()
            return text or str(issue.get("summary") or "No summary provided")
        except Exception:
            logging.exception("Agent summarize_issue_for_standup failed for issue=%s", issue.get("key"))
            return str(issue.get("summary") or "No summary provided")

    def get_issue_transitions(self, issue_key: str) -> List[Dict[str, Any]]:
        if not issue_key or not self._is_foundry_configured():
            return []
        prompt = (
            f"Use the Jira MCP function get_issue_transitions for issue {issue_key}. "
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

    def update_issue_status(
        self,
        issue_key: str,
        *,
        transition_name: str | None = None,
        transition_id: str | None = None,
    ) -> Dict[str, Any]:
        if not issue_key:
            return {"ok": False, "reason": "missing_issue_key"}
        if not self._is_foundry_configured():
            return {"ok": False, "reason": "agent_not_configured"}
        if not transition_name and not transition_id:
            return {"ok": False, "reason": "missing_transition"}

        transition_desc = f"transition_id={transition_id}" if transition_id else f"transition_name={transition_name}"
        prompt = (
            f"Use the Jira MCP function update_issue_status for issue {issue_key} with {transition_desc}. "
            "Return only JSON with keys: ok, issue_key, transition_applied, message."
        )
        try:
            data = self._call_foundry(prompt)
            parsed = self._extract_first_json_object(data)
            if parsed:
                return {
                    "ok": bool(parsed.get("ok", True)),
                    "issue_key": str(parsed.get("issue_key") or issue_key),
                    "transition_applied": str(parsed.get("transition_applied") or transition_name or transition_id or ""),
                    "message": str(parsed.get("message") or ""),
                }
            text = self._extract_plain_text(data).strip()
            return {
                "ok": True,
                "issue_key": issue_key,
                "transition_applied": transition_name or transition_id or "",
                "message": text,
            }
        except Exception as exc:
            logging.exception("Agent update_issue_status failed for issue=%s", issue_key)
            return {"ok": False, "issue_key": issue_key, "transition_applied": transition_name or transition_id or "", "message": str(exc)}

    def create_issue_intro(self, issue: Dict[str, Any], position: int | None = None, total: int | None = None) -> str:
        assignee = issue.get("assignee") or "the team"
        issue_key = issue.get("key") or "Unknown issue"
        summary = self.summarize_issue_for_standup(issue)
        status = issue.get("status") or "Unknown"

        ordinal = ""
        if position is not None and total is not None:
            ordinal = f"Issue {position} of {total}. "

        if status == "Blocked":
            status_phrase = "is currently blocked"
        else:
            status_phrase = f"is currently {status.lower()}"

        narration = (
            f"{ordinal}Next is {issue_key}: {summary}. "
            f"This item {status_phrase}. "
            f"{assignee}, please share your update."
        )
        logging.info("Agent narration built for issue=%s", issue_key)
        return narration

    def create_transition_text(self, next_issue: Dict[str, Any], position: int, total: int) -> str:
        return self.create_issue_intro(next_issue, position=position, total=total)

    def create_closing_text(self, processed_count: int) -> str:
        if processed_count <= 0:
            return "There are no blocked or in progress issues to review today."
        return f"That completes the standup review. We went through {processed_count} issues."


    def _sort_issues_for_dashboard(self, issues: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        order = {"Blocked": 0, "In Progress": 1, "In Review": 2}
        sorted_issues = sorted(
            issues,
            key=lambda issue: (order.get(str(issue.get("status") or ""), 99), str(issue.get("priority") or ""), str(issue.get("key") or "")),
        )
        return sorted_issues

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
            for issue in parsed:
                if not isinstance(issue, dict):
                    continue
                normalized = {
                    "key": issue.get("key", "N/A"),
                    "status": issue.get("status", "Unknown"),
                    "assignee": issue.get("assignee", "Unassigned"),
                    "summary": issue.get("summary", "No Summary Provided"),
                    "priority": issue.get("priority", "Unknown"),
                    "description": issue.get("description", ""),
                    "id": issue.get("id", ""),
                    "name": issue.get("name", ""),
                    "to_status": issue.get("to_status", issue.get("name", "")),
                }
                extracted_items.append(normalized)
            if extracted_items:
                break

        if not extracted_items:
            logging.warning("No JSON issue array could be extracted from foundry response")
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
