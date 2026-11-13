"""Thin wrapper around the Genie Conversation API (runs as the app's service principal)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient

SPACE_ID = os.getenv("GENIE_SPACE_ID", "")
_w: WorkspaceClient | None = None


def client() -> WorkspaceClient:
    global _w
    if _w is None:
        _w = WorkspaceClient()  # picks up app service-principal credentials injected by Databricks Apps
    return _w


@dataclass
class GenieAnswer:
    text: str = ""
    sql: str | None = None
    columns: list[str] = field(default_factory=list)
    rows: list[list] = field(default_factory=list)


def ask(question: str, conversation_id: str | None = None, max_rows: int = 50) -> tuple[GenieAnswer, str]:
    """Return (answer, conversation_id). Pass conversation_id to keep follow-up context."""
    if not SPACE_ID:
        raise RuntimeError("GENIE_SPACE_ID is not configured")
    w = client()
    if conversation_id:
        msg = w.genie.create_message_and_wait(SPACE_ID, conversation_id, question)
    else:
        msg = w.genie.start_conversation_and_wait(SPACE_ID, question)
    ans = GenieAnswer()
    for att in msg.attachments or []:
        if att.text and att.text.content:
            ans.text += att.text.content + "\n"
        if att.query:
            ans.sql = att.query.query
            ans.text += (att.query.description or "") + "\n"
            res = w.genie.get_message_attachment_query_result(
                SPACE_ID, msg.conversation_id, msg.message_id, att.attachment_id)
            sr = res.statement_response
            if sr and sr.manifest and sr.manifest.schema and sr.manifest.schema.columns:
                ans.columns = [c.name for c in sr.manifest.schema.columns]
            if sr and sr.result and sr.result.data_array:
                ans.rows = sr.result.data_array[:max_rows]
    ans.text = ans.text.strip() or "Genie did not return an answer."
    return ans, msg.conversation_id


def as_plain_text(ans: GenieAnswer, max_rows: int = 10) -> str:
    out = [ans.text]
    if ans.columns and ans.rows:
        out.append(" | ".join(ans.columns))
        out += [" | ".join(str(c) for c in r) for r in ans.rows[:max_rows]]
    return "\n".join(out)
