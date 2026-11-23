"""HTTP contract backed by the real PostgreSQL store."""
from uuid import uuid4

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("psycopg")
from fastapi.testclient import TestClient

from api.app import create_app

pytestmark = pytest.mark.postgres


@pytest.fixture
def client(db_store):
    with TestClient(create_app(db_store.dsn)) as client:
        yield client


def test_submit_get_list_and_openapi(client, db_event):
    assert client.get("/health/ready").json() == {"status": "ready"}
    created = client.post("/v1/runs", json=db_event)
    assert created.status_code == 201, created.text
    assert created.json()["outcome"] == "created"
    duplicate = client.post("/v1/runs", json=db_event)
    assert duplicate.status_code == 200 and duplicate.json()["outcome"] == "unchanged"
    assert client.get(f"/v1/runs/{db_event['pipeline_id']}/{db_event['run_id']}").json() == db_event
    page = client.get("/v1/runs", params={"pipeline_id": db_event["pipeline_id"], "status": "completed"}).json()
    assert page == {"items": [db_event], "limit": 20, "offset": 0}
    schema = client.get("/openapi.json").json()
    assert schema["paths"]["/v1/runs"]["post"]["requestBody"]["required"] is True
    assert "QualityMetric" in schema["components"]["schemas"]
    assert client.get("/docs").status_code == 200


def test_lifecycle_and_conflict(client, db_event):
    running = db_event | {"status": "running", "finished_at": None, "duration_ms": None}
    assert client.post("/v1/runs", json=running).status_code == 201
    updated = client.post("/v1/runs", json=db_event)
    assert updated.status_code == 200 and updated.json()["outcome"] == "updated"
    assert client.post("/v1/runs", json=running).status_code == 409
    assert client.post("/v1/runs", json=db_event | {"duration_ms": 5}).status_code == 409


@pytest.mark.parametrize("change", [{"duration_ms": True}, {"extra": 1}, {"status": "success"},
                                    {"row_counts": {"input_count": -1}}, {"contract_version": "2"},
                                    {"started_at": "0001-01-01T00:00:00+05:30"}])
def test_invalid_event_does_not_write(client, db_store, db_event, change):
    response = client.post("/v1/runs", json=db_event | change)
    assert response.status_code == 422
    assert db_store.get(db_event["pipeline_id"], db_event["run_id"]) is None


@pytest.mark.parametrize("body", ['{"status":"running","status":"failed"}', '{"rate":NaN}', '{broken', b'\xff', b'{"x":"\\ud800"}'])
def test_malformed_json(client, body):
    assert client.post("/v1/runs", content=body, headers={"content-type": "application/json"}).status_code == 400


def test_body_limit_and_content_type(client):
    assert client.post("/v1/runs", content="{}").status_code == 415
    response = client.post("/v1/runs", content=b" " * (1024 * 1024 + 1), headers={"content-type": "application/json"})
    assert response.status_code == 413


def test_unrepresentable_database_text_is_rejected_atomically(client, db_store, db_event):
    db_event["properties"]["note"] = "bad\x00text"
    assert client.post("/v1/runs", json=db_event).status_code == 422
    assert db_store.get(db_event["pipeline_id"], db_event["run_id"]) is None


def test_query_errors_and_missing_execution(client, db_event):
    assert client.get(f"/v1/runs/{db_event['pipeline_id']}/{uuid4()}").status_code == 404
    assert client.get("/v1/runs/retail/not-a-uuid").status_code == 422
    for query in ("limit=101", "limit=0", "offset=-1", "status=success", "pipeline_id=bad%20name"):
        assert client.get("/v1/runs?" + query).status_code == 422


def test_database_unavailable_does_not_leak_connection_details(tmp_path):
    with TestClient(create_app(f"host={tmp_path}/missing dbname=private_name password=secret")) as client:
        response = client.get("/health/ready")
        assert response.status_code == 503
        assert "secret" not in response.text and "private_name" not in response.text


def test_missing_configuration_fails_clearly(monkeypatch):
    monkeypatch.delenv("PIPELINEIQ_DATABASE_URL", raising=False)
    with pytest.raises(ValueError, match="PIPELINEIQ_DATABASE_URL"):
        create_app()
