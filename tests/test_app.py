import main
from fastapi.testclient import TestClient
from genie_client import GenieAnswer


def test_health_and_ask(monkeypatch):
    monkeypatch.setattr(main.genie_client, "ask",
                        lambda q, c=None: (GenieAnswer(text="ok", sql="select 1", columns=["a"], rows=[["1"]]), "conv1"))
    c = TestClient(main.app)
    assert c.get("/healthz").status_code == 200
    r = c.post("/api/ask", json={"question": "hi"})
    assert r.json()["conversation_id"] == "conv1" and r.json()["rows"] == [["1"]]


def test_slack_rejects_unsigned():
    assert TestClient(main.app).post("/slack/commands", content=b"text=x").status_code == 401
