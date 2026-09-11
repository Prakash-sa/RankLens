"""FastAPI adapter for RankLens enterprise ingestion."""

from __future__ import annotations

import argparse
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from ranklens import __version__

from .auth import MachineAuthenticator
from .contracts import DurableReceipt, HealthStatus, ReceiptView, SegmentUpload
from .database import assert_schema_ready, build_engine, build_session_factory, initialize_schema
from .ingestion import AdmissionError, IngestionService
from .settings import MachinePrincipal, Settings
from .storage import LocalObjectStore


def create_app(settings: Settings) -> FastAPI:
    engine = build_engine(settings.database_url)
    sessions = build_session_factory(engine)
    objects = LocalObjectStore(settings.object_root)
    service = IngestionService(
        sessions, objects, max_expanded_segment_bytes=settings.max_expanded_segment_bytes
    )
    authenticator = MachineAuthenticator(settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if settings.bootstrap_schema:
            initialize_schema(engine)
        else:
            assert_schema_ready(engine)
        yield
        engine.dispose()

    app = FastAPI(
        title="RankLens Enterprise API",
        version=__version__,
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.ingestion = service

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        request.state.request_id = request_id
        if request.url.path == "/v1/segments" and request.method == "POST":
            content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
            if content_type != "application/json":
                return JSONResponse(
                    status_code=415,
                    content={
                        "code": "unsupported_content_type",
                        "message": "segment admission requires application/json",
                        "retriable": False,
                        "request_id": request_id,
                    },
                    headers={"x-request-id": request_id, "cache-control": "no-store"},
                )
            raw_length = request.headers.get("content-length")
            try:
                content_length = int(raw_length) if raw_length is not None else -1
            except ValueError:
                content_length = -1
            if content_length < 0 or content_length > 13 * 1024 * 1024:
                return JSONResponse(
                    status_code=413,
                    content={
                        "code": "request_size_rejected",
                        "message": "a valid bounded Content-Length is required",
                        "retriable": False,
                        "request_id": request_id,
                    },
                    headers={"x-request-id": request_id, "cache-control": "no-store"},
                )
        response = await call_next(request)
        response.headers["x-request-id"] = request_id
        response.headers["cache-control"] = "no-store"
        return response

    @app.exception_handler(AdmissionError)
    async def admission_error_handler(request: Request, error: AdmissionError):
        return JSONResponse(
            status_code=error.status_code,
            content={
                "code": error.code,
                "message": str(error),
                "retriable": error.retriable,
                "request_id": request.state.request_id,
            },
        )

    @app.get("/healthz", response_model=HealthStatus)
    def health() -> HealthStatus:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return HealthStatus()

    @app.post("/v1/segments", response_model=DurableReceipt, status_code=201)
    def admit_segment(
        segment: SegmentUpload,
        principal: MachinePrincipal = Depends(authenticator.authenticate),
    ) -> DurableReceipt:
        return service.admit(principal, segment)

    @app.get("/v1/receipts/{receipt_id}", response_model=ReceiptView)
    def receipt(
        receipt_id: str,
        principal: MachinePrincipal = Depends(authenticator.authenticate),
    ) -> ReceiptView:
        result = service.get_receipt(principal.tenant_id, receipt_id)
        if result is None or result.cluster_id not in principal.clusters:
            raise HTTPException(status_code=404, detail={"code": "receipt_not_found"})
        return result

    return app


def application() -> FastAPI:
    return create_app(Settings.from_environment())


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the RankLens enterprise ingestion API")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    arguments = parser.parse_args()
    import uvicorn

    uvicorn.run(
        "ranklens_enterprise.api:application",
        factory=True,
        host=arguments.host,
        port=arguments.port,
        proxy_headers=False,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
