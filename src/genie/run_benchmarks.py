"""Score Genie answers against the benchmark SQL (accuracy regression test for the space).

For each benchmark: ask Genie the question via the Conversation API, fetch the SQL result it produced,
run the expected SQL on the warehouse, and compare result sets (order-insensitive, numeric tolerance).

Usage: python run_benchmarks.py --space-id <id> --catalog retail_dev --warehouse-id <id> [--min-accuracy 0.8]
Exit code 1 if accuracy < --min-accuracy (usable as a CI/CD gate).
"""
import argparse
import json
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient


def norm(rows):
    def cell(v):
        try:
            return round(float(v), 2)
        except (TypeError, ValueError):
            return str(v)
    return sorted(tuple(cell(c) for c in r) for r in (rows or []))


def run_sql(w, warehouse_id, sql):
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=warehouse_id, wait_timeout="50s")
    return (r.result.data_array if r.result else None) or []


def ask_genie(w, space_id, question):
    msg = w.genie.start_conversation_and_wait(space_id=space_id, content=question)
    for att in msg.attachments or []:
        if att.query:
            res = w.genie.get_message_attachment_query_result(
                space_id=space_id, conversation_id=msg.conversation_id,
                message_id=msg.message_id, attachment_id=att.attachment_id)
            return att.query.query, (res.statement_response.result.data_array or [])
    return None, []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--space-id", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--warehouse-id", required=True)
    ap.add_argument("--min-accuracy", type=float, default=0.8)
    ap.add_argument("--config", default=str(Path(__file__).with_name("genie_space_config.json")))
    a = ap.parse_args()
    cfg = json.loads(Path(a.config).read_text())
    w = WorkspaceClient()
    passed, report = 0, []
    for b in cfg["benchmarks"]:
        expected = norm(run_sql(w, a.warehouse_id, b["sql"].format(catalog=a.catalog)))
        try:
            genie_sql, got = ask_genie(w, a.space_id, b["question"])
        except Exception as e:
            genie_sql, got = f"ERROR: {e}", []
        ok = norm(got) == expected
        passed += ok
        report.append({"question": b["question"], "passed": ok, "genie_sql": genie_sql})
        print(("PASS" if ok else "FAIL"), "-", b["question"])
    acc = passed / len(cfg["benchmarks"])
    Path("genie_benchmark_report.json").write_text(json.dumps({"accuracy": acc, "results": report}, indent=2))
    print(f"accuracy = {acc:.0%}")
    return 0 if acc >= a.min_accuracy else 1


if __name__ == "__main__":
    sys.exit(main())
