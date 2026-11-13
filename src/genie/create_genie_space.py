"""Create (or update) the Genie space on Gold datasets, including benchmark questions.

Uses the Genie Spaces REST API (Public Preview - the `serialized_space` schema may evolve; check the
current docs if the call is rejected). Usage:
  python create_genie_space.py --catalog retail_dev --warehouse-id <id> [--parent-path /Workspace/Users/me]
Prints the space_id -> set it as the `genie_space_id` bundle variable.
"""
import argparse
import json
import uuid
from pathlib import Path

from databricks.sdk import WorkspaceClient


def hexid() -> str:
    return uuid.uuid4().hex


def build_serialized_space(cfg: dict, catalog: str) -> str:
    space = {
        "version": 2,
        "config": {"sample_questions": [{"id": hexid(), "question": [q]} for q in cfg["sample_questions"]]},
        "data_sources": {"tables": [{"identifier": f"{catalog}.{t}"} for t in sorted(cfg["tables"])]},
        "instructions": {"text_instructions": [{"id": hexid(), "content": cfg["instructions"]}]},
        "benchmarks": {"questions": [
            {"id": hexid(), "question": [b["question"]],
             "answer": [{"format": "SQL", "content": [b["sql"].format(catalog=catalog)]}]}
            for b in cfg["benchmarks"]]},
    }
    return json.dumps(space)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--warehouse-id", required=True)
    ap.add_argument("--parent-path", default=None)
    ap.add_argument("--config", default=str(Path(__file__).with_name("genie_space_config.json")))
    a = ap.parse_args()
    cfg = json.loads(Path(a.config).read_text())
    w = WorkspaceClient()
    body = {"title": cfg["title"], "description": cfg["description"], "warehouse_id": a.warehouse_id,
            "serialized_space": build_serialized_space(cfg, a.catalog)}
    if a.parent_path:
        body["parent_path"] = a.parent_path
    res = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)
    print("space_id:", res.get("space_id"))


if __name__ == "__main__":
    main()
