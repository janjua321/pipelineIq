"""Transactional PostgreSQL storage for generic execution snapshots."""
import hashlib
import os
from importlib.resources import files

import psycopg
from psycopg.types.json import Jsonb

from monitoring.events import STATUSES, validate_event


class EventConflict(ValueError):
    """An execution identity already contains incompatible evidence."""


def database_url() -> str:
    value = os.environ.get("PIPELINEIQ_DATABASE_URL")
    if not value:
        raise ValueError("Set PIPELINEIQ_DATABASE_URL; see docs/monitoring-storage.md")
    return value


def connect(dsn: str):
    return psycopg.connect(dsn, connect_timeout=5,
                           options="-c search_path=public -c statement_timeout=10000 -c lock_timeout=5000")


def migrate(dsn: str) -> list[str]:
    """Apply bundled SQL once, atomically, rejecting changes to applied files."""
    applied = []
    with connect(dsn) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(748233901)")
        conn.execute("""CREATE TABLE IF NOT EXISTS schema_migrations (
            version text PRIMARY KEY, sha256 text NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT now())""")
        saved = dict(conn.execute("SELECT version, sha256 FROM schema_migrations").fetchall())
        migrations = sorted(files("monitoring").joinpath("migrations").iterdir(), key=lambda path: path.name)
        names = {path.name for path in migrations if path.name.endswith('.sql')}
        if saved.keys() - names:
            raise ValueError("Database has migrations unknown to this application")
        for path in migrations:
            if path.name not in names:
                continue
            content = path.read_bytes()
            checksum = hashlib.sha256(content).hexdigest()
            if path.name in saved:
                if saved[path.name] != checksum:
                    raise ValueError(f"Applied migration changed: {path.name}")
                continue
            conn.execute(content.decode("utf-8"))
            conn.execute("INSERT INTO schema_migrations (version, sha256) VALUES (%s, %s)", (path.name, checksum))
            applied.append(path.name)
    return applied


def _write_metrics(conn, event: dict) -> None:
    identity = (event["pipeline_id"], event["run_id"])
    conn.execute("DELETE FROM row_counts WHERE pipeline_id=%s AND run_id=%s", identity)
    conn.execute("DELETE FROM quality_metrics WHERE pipeline_id=%s AND run_id=%s", identity)
    with conn.cursor() as cursor:
        cursor.executemany("INSERT INTO row_counts VALUES (%s, %s, %s, %s)",
                           [(*identity, name, count) for name, count in event["row_counts"].items()])
        cursor.executemany("INSERT INTO quality_metrics VALUES (%s, %s, %s, %s, %s, %s)",
                           [(*identity, name, metric["count"], metric["denominator"], metric["rate"])
                            for name, metric in event["quality_metrics"].items()])


class EventStore:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def submit(self, value: object) -> str:
        event = validate_event(value)
        identity = (event["pipeline_id"], event["run_id"])
        with connect(self.dsn) as conn:
            inserted = conn.execute("""INSERT INTO executions
                (pipeline_id, run_id, batch_id, status, started_at, finished_at, event)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pipeline_id, run_id) DO NOTHING RETURNING run_id""",
                (*identity, event["batch_id"], event["status"], event["started_at"],
                 event["finished_at"], Jsonb(event))).fetchone()
            if inserted:
                outcome = "created"
            else:
                old = conn.execute("SELECT event FROM executions WHERE pipeline_id=%s AND run_id=%s FOR UPDATE",
                                   identity).fetchone()[0]
                if old == event:
                    return "unchanged"
                if old["status"] != "running" or event["status"] == "running":
                    raise EventConflict("Only identical redelivery or running-to-terminal updates are allowed")
                if any(old[name] != event[name] for name in ("contract_version", "batch_id", "started_at")):
                    raise EventConflict("Execution contract, batch and start time cannot change")
                conn.execute("""UPDATE executions SET status=%s, finished_at=%s, event=%s, updated_at=now()
                    WHERE pipeline_id=%s AND run_id=%s""",
                    (event["status"], event["finished_at"], Jsonb(event), *identity))
                outcome = "updated"
            _write_metrics(conn, event)
        return outcome

    def get(self, pipeline_id: str, run_id: str) -> dict | None:
        with connect(self.dsn) as conn:
            row = conn.execute("SELECT event FROM executions WHERE pipeline_id=%s AND run_id=%s",
                               (pipeline_id, run_id)).fetchone()
        return row[0] if row else None

    def list(self, *, pipeline_id: str | None = None, status: str | None = None,
             limit: int = 20, offset: int = 0) -> list[dict]:
        if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or not 0 <= offset <= 100000:
            raise ValueError("limit must be 1–100 and offset 0–100000")
        if status is not None and status not in STATUSES:
            raise ValueError("Unknown status")
        clauses, params = [], []
        for name, value in (("pipeline_id", pipeline_id), ("status", status)):
            if value is not None:
                clauses.append(f"{name}=%s")  # Column names come only from this fixed tuple.
                params.append(value)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with connect(self.dsn) as conn:
            rows = conn.execute("SELECT event FROM executions" + where +
                                " ORDER BY started_at DESC, pipeline_id, run_id LIMIT %s OFFSET %s",
                                (*params, limit, offset)).fetchall()
        return [row[0] for row in rows]

    def ready(self) -> bool:
        with connect(self.dsn) as conn:
            conn.execute("SELECT 1 FROM executions LIMIT 0")
            conn.execute("SELECT 1 FROM row_counts LIMIT 0")
            conn.execute("SELECT 1 FROM quality_metrics LIMIT 0")
        return True
