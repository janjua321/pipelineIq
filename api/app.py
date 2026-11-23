"""Minimal local API. Start with uvicorn api.app:create_app --factory."""
import json
from typing import Annotated
from uuid import UUID

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from api.models import ExecutionEvent, RunPage, Status, Submission
from monitoring.events import loads_json
from monitoring.store import EventConflict, EventStore, database_url

MAX_BODY_BYTES = 1024 * 1024
PIPELINE_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"


async def parse_event(request: Request) -> ExecutionEvent:
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
        raise HTTPException(415, "Content-Type must be application/json")
    chunks, size = [], 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > MAX_BODY_BYTES:
            raise HTTPException(413, "Event exceeds 1 MiB")
        chunks.append(chunk)
    try:
        value = loads_json(b"".join(chunks).decode("utf-8"))
        # Escaped lone surrogates parse as Python text but cannot be sent as UTF-8.
        json.dumps(value, ensure_ascii=False).encode("utf-8")
    except (ValueError, UnicodeError, RecursionError):
        raise HTTPException(400, "Malformed JSON, duplicate keys or non-finite constants") from None
    try:
        return ExecutionEvent.model_validate(value)
    except ValidationError as error:
        raise HTTPException(422, error.errors(include_url=False, include_context=False, include_input=False)) from None


def create_app(dsn: str | None = None) -> FastAPI:
    app = FastAPI(title="PipelineIQ monitoring", version="0.1.0",
                  description="Local contract-1 execution submission and queries. No Spark execution.")
    store = EventStore(dsn if dsn is not None else database_url())

    @app.exception_handler(EventConflict)
    async def conflict(request, error):
        return JSONResponse(status_code=409, content={"detail": str(error)})

    @app.exception_handler(psycopg.DataError)
    async def invalid_database_value(request, error):
        return JSONResponse(status_code=422, content={"detail": "Event contains a value PostgreSQL cannot represent"})

    @app.exception_handler(psycopg.Error)
    async def database_failure(request, error):
        return JSONResponse(status_code=503, content={"detail": "Database unavailable or schema not ready"})

    @app.get("/health/ready")
    def ready():
        store.ready()
        return {"status": "ready"}

    @app.post("/v1/runs", response_model=Submission, status_code=201,
              responses={200: {"model": Submission}, 409: {"description": "Conflicting execution evidence"}},
              openapi_extra={"requestBody": {"required": True, "content": {
                  "application/json": {"schema": {"$ref": "#/components/schemas/ExecutionEvent"}}
              }}})
    def submit(event: Annotated[ExecutionEvent, Depends(parse_event)], response: Response):
        outcome = store.submit(event.model_dump())
        response.status_code = 201 if outcome == "created" else 200
        return {"outcome": outcome, "pipeline_id": event.pipeline_id, "run_id": event.run_id}

    @app.get("/v1/runs", response_model=RunPage)
    def list_runs(pipeline_id: Annotated[str | None, Query(pattern=PIPELINE_PATTERN)] = None,
                  status: Status | None = None, limit: Annotated[int, Query(ge=1, le=100)] = 20,
                  offset: Annotated[int, Query(ge=0, le=100000)] = 0):
        return {"items": store.list(pipeline_id=pipeline_id, status=status, limit=limit, offset=offset),
                "limit": limit, "offset": offset}

    @app.get("/v1/runs/{pipeline_id}/{run_id}", response_model=ExecutionEvent)
    def get_run(pipeline_id: str, run_id: UUID):
        event = store.get(pipeline_id, str(run_id))
        if event is None:
            raise HTTPException(404, "Execution not found")
        return event

    # The strict raw-JSON dependency catches duplicate keys before Pydantic can see
    # them. Publish its body model explicitly so /docs still offers a typed editor.
    def openapi():
        if app.openapi_schema is None:
            schema = get_openapi(title=app.title, version=app.version, routes=app.routes)
            model = ExecutionEvent.model_json_schema(ref_template="#/components/schemas/{model}")
            components = schema.setdefault("components", {}).setdefault("schemas", {})
            components.update(model.pop("$defs", {}))
            components["ExecutionEvent"] = model
            app.openapi_schema = schema
        return app.openapi_schema
    app.openapi = openapi
    return app
