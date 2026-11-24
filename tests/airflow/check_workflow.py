"""Real DAG/Spark/PostgreSQL checks. Run using .venv Python and an explicit test DSN."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
from uuid import uuid4

from psycopg.conninfo import conninfo_to_dict
from monitoring.store import connect, migrate
from pipelines.generate import GenerationConfig, write_batch

project = Path(__file__).resolve().parents[2]
dsn = os.environ["PIPELINEIQ_TEST_DATABASE_URL"]
assert conninfo_to_dict(dsn).get("dbname", "").endswith("_test"), "Use a dedicated _test database"
migrate(dsn)
root = project / "data" / "airflow-checks" / uuid4().hex
root.mkdir(parents=True)
environment = os.environ | {"PIPELINEIQ_DATABASE_URL": dsn, "PIPELINEIQ_PIPELINE_ROOT": str(root / "pipeline"),
                            "PIPELINEIQ_ORCHESTRATION_ROOT": str(root / "outbox")}


def dag_states():
    with sqlite3.connect(project / "data/airflow/airflow.db") as conn:
        return dict(conn.execute("SELECT run_id, state FROM dag_run WHERE dag_id='pipelineiq_retail'"))


def run(label, batch, source, expected, recover=False):
    before = dag_states()
    with (root / f"{label}.log").open("w") as log:
        process = subprocess.Popen(["bash", "scripts/airflow.sh", "dags", "test", "pipelineiq_retail",
                                    "--conf", json.dumps({"source": str(source), "batch_id": batch})],
                                   cwd=project, env=environment, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 240
        try:
            while process.poll() is None:
                if time.monotonic() > deadline:
                    raise TimeoutError(f"DAG check timed out: {label}; inspect {root}")
                if recover and not source.exists():
                    for path in (root / "outbox").glob("*/attempt-*/event.json"):
                        event = json.loads(path.read_text())
                        if event["batch_id"] == batch and event["status"] == "failed":
                            write_batch(GenerationConfig(batch_id=batch, rows=6), source.parent)
                            break
                time.sleep(.1)
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=20)
    after = dag_states()
    run_ids = after.keys() - before.keys()
    assert len(run_ids) == 1, (label, root)
    run_id = run_ids.pop()
    assert after[run_id] == expected, (label, after[run_id], root)
    with connect(dsn) as conn:
        events = [row[0] for row in conn.execute("""SELECT event FROM executions
            WHERE event->'properties'->>'orchestration_run_id'=%s
            ORDER BY (event->'properties'->>'orchestration_attempt')::integer""",
            ("pipelineiq_retail:" + run_id,)).fetchall()]
    print(label, "dag_state=" + after[run_id], "events=" + str([event["status"] for event in events]), flush=True)
    return events


healthy = write_batch(GenerationConfig(batch_id="healthy", rows=8), root / "source")
first = run("healthy", "healthy", healthy, "success")
assert len(first) == 1 and first[0]["row_counts"]["valid_count"] == 8
reused = run("reused", "healthy", healthy, "success")
assert reused[0]["status"] == "reused" and reused[0]["reused_from_run_id"] == first[0]["run_id"]
failed = run("permanent_failure", "missing", root / "source/missing.csv", "failed")
assert [event["status"] for event in failed] == ["failed", "failed"]
recovered = run("recovered", "recover", root / "source/recover.csv", "success", recover=True)
assert [event["status"] for event in recovered] == ["failed", "completed"]
assert recovered[1]["row_counts"]["valid_count"] == 6
assert len(list((root / "pipeline/attempts").iterdir())) == 2
print("Four real DAG scenarios verified; retained logs and outputs:", root)
