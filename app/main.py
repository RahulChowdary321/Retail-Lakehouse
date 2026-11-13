"""Retail Genie App - Databricks App (FastAPI).

  GET  /                chat UI for business users
  POST /api/ask         JSON API {question, conversation_id?}
  POST /slack/commands  Slack slash command  (/retail <question>)
  POST /teams/messages  Teams outgoing webhook (@Retail <question>)
  GET  /healthz         liveness probe
"""
import json
import logging
import os
import urllib.parse

import genie_client
import httpx
import uvicorn
from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from security import verify_slack, verify_teams

log = logging.getLogger("retail-genie-app")
app = FastAPI(title="Retail Genie App")


class AskRequest(BaseModel):
    question: str
    conversation_id: str | None = None


@app.get("/healthz")
def healthz():
    return {"status": "ok", "genie_space_configured": bool(genie_client.SPACE_ID)}


@app.post("/api/ask")
def api_ask(req: AskRequest):
    try:
        ans, conv = genie_client.ask(req.question, req.conversation_id)
    except Exception as e:
        log.exception("genie failure")
        raise HTTPException(502, f"Genie request failed: {e}") from e
    return {"conversation_id": conv, "text": ans.text, "sql": ans.sql, "columns": ans.columns, "rows": ans.rows}


# ---------------------------------------------------------------- Slack
def _slack_reply(response_url: str, question: str) -> None:
    try:
        ans, _ = genie_client.ask(question)
        text = f"*Q:* {question}\n```{genie_client.as_plain_text(ans)}```"
    except Exception as e:
        text = f":warning: Genie failed: {e}"
    httpx.post(response_url, json={"response_type": "in_channel", "text": text}, timeout=15)


@app.post("/slack/commands")
async def slack_commands(request: Request, background: BackgroundTasks,
                         x_slack_request_timestamp: str = Header(""), x_slack_signature: str = Header("")):
    body = await request.body()
    if not verify_slack(body, x_slack_request_timestamp, x_slack_signature):
        raise HTTPException(401, "invalid signature")
    form = urllib.parse.parse_qs(body.decode())
    question = form.get("text", [""])[0].strip()
    if not question:
        return {"response_type": "ephemeral", "text": "Usage: /retail <your question>"}
    background.add_task(_slack_reply, form["response_url"][0], question)  # Slack requires an ack within 3s
    return {"response_type": "ephemeral", "text": f":hourglass_flowing_sand: Asking Genie: _{question}_"}


# ---------------------------------------------------------------- Teams
@app.post("/teams/messages")
async def teams_messages(request: Request, authorization: str = Header("")):
    body = await request.body()
    if not verify_teams(body, authorization):
        raise HTTPException(401, "invalid HMAC")
    text = json.loads(body).get("text", "")
    question = text.replace("<at>Retail</at>", "").strip()  # strip the @mention
    try:
        ans, _ = genie_client.ask(question)
        reply = genie_client.as_plain_text(ans).replace("\n", "<br>")
    except Exception as e:
        reply = f"Genie failed: {e}"
    return JSONResponse({"type": "message", "text": reply})


# ---------------------------------------------------------------- UI
PAGE = """<!doctype html><meta charset=utf-8><title>Retail Assistant</title>
<style>body{font:16px system-ui;max-width:860px;margin:2rem auto;padding:0 1rem}
#log div{margin:.6rem 0;padding:.6rem .8rem;border-radius:8px;background:#f3f4f6;white-space:pre-wrap}
.q{background:#e0ecff!important}table{border-collapse:collapse;margin-top:.4rem}td,th{border:1px solid #ccc;padding:2px 8px}
input{width:75%;padding:.5rem}button{padding:.5rem 1rem}details{margin-top:.3rem}</style>
<h2>Retail Sales Assistant</h2><div id=log></div>
<p><input id=q placeholder="e.g. Top 5 categories by net revenue this month" onkeydown="if(event.key=='Enter')go()">
<button onclick=go()>Ask</button></p>
<script>
let conv=null;const log=document.getElementById('log');
function add(h,c){const d=document.createElement('div');if(c)d.className=c;d.innerHTML=h;log.appendChild(d);return d}
const esc=s=>String(s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
async function go(){const i=document.getElementById('q'),q=i.value.trim();if(!q)return;i.value='';
add(esc(q),'q');const w=add('Thinking…');
try{const r=await fetch('api/ask',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({question:q,conversation_id:conv})});
const j=await r.json();if(!r.ok)throw new Error(j.detail);conv=j.conversation_id;
let h=esc(j.text);if(j.rows.length){h+='<table><tr>'+j.columns.map(c=>'<th>'+esc(c)+'</th>').join('')+'</tr>'+
j.rows.map(r=>'<tr>'+r.map(c=>'<td>'+esc(c)+'</td>').join('')+'</tr>').join('')+'</table>'}
if(j.sql)h+='<details><summary>SQL</summary><pre>'+esc(j.sql)+'</pre></details>';w.innerHTML=h}
catch(e){w.textContent='Error: '+e.message}}
</script>"""


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("DATABRICKS_APP_PORT", os.getenv("PORT", "8000"))))
