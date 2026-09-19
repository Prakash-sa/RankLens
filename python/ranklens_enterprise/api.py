"""FastAPI adapter for RankLens enterprise ingestion."""

from __future__ import annotations

import argparse
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import List, Optional

from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy import text

from ranklens import __version__

from .auth import MachineAuthenticator
from .contracts import (
    AllocationTimelineView,
    DurableReceipt,
    HealthStatus,
    ReceiptView,
    ReportRevisionView,
    SchedulerObservationView,
    SegmentUpload,
)
from .database import (
    AttemptGeneration,
    ReportHead,
    ReportRevision,
    SchedulerObservation,
    assert_schema_ready,
    build_engine,
    build_session_factory,
    initialize_schema,
)
from .ingestion import AdmissionError, IngestionService
from .scheduler import derive_allocation_timeline
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

    @app.get("/v1/scheduler/observations", response_model=List[SchedulerObservationView])
    def scheduler_observations(
        cluster_id: str = Query(min_length=1, max_length=128),
        job_id: Optional[str] = Query(default=None, min_length=1, max_length=128),
        limit: int = Query(default=100, ge=1, le=500),
        principal: MachinePrincipal = Depends(authenticator.authenticate),
    ) -> List[SchedulerObservationView]:
        if cluster_id not in principal.clusters:
            raise HTTPException(status_code=404, detail={"code": "cluster_not_found"})
        statement = select(SchedulerObservation).where(
            SchedulerObservation.tenant_id == principal.tenant_id,
            SchedulerObservation.cluster_id == cluster_id,
        )
        if job_id is not None:
            statement = statement.where(SchedulerObservation.job_id == job_id)
        statement = statement.order_by(SchedulerObservation.observed_at.desc()).limit(limit)
        with sessions() as session:
            rows = session.execute(statement).scalars().all()
            return [
                SchedulerObservationView(
                    observation_id=row.observation_id,
                    tenant_id=row.tenant_id,
                    cluster_id=row.cluster_id,
                    adapter=row.adapter,
                    adapter_version=row.adapter_version,
                    source_identity=row.source_identity,
                    job_id=row.job_id,
                    array_job_id=row.array_job_id,
                    array_task_id=row.array_task_id,
                    step_id=row.step_id,
                    restart_count=row.restart_count,
                    state=row.state,
                    state_reason=row.state_reason,
                    submitted_at=row.submitted_at,
                    started_at=row.started_at,
                    ended_at=row.ended_at,
                    allocated_nodes=row.allocated_nodes,
                    allocated_cpus=row.allocated_cpus,
                    account=row.account,
                    partition=row.partition,
                    user_ref=row.user_ref,
                    observed_at=row.observed_at,
                    first_seen_at=row.first_seen_at,
                )
                for row in rows
            ]

    @app.get("/v1/scheduler/allocation", response_model=AllocationTimelineView)
    def scheduler_allocation(
        cluster_id: str = Query(min_length=1, max_length=128),
        source_identity: str = Query(min_length=1, max_length=256),
        principal: MachinePrincipal = Depends(authenticator.authenticate),
    ) -> AllocationTimelineView:
        if cluster_id not in principal.clusters:
            raise HTTPException(status_code=404, detail={"code": "cluster_not_found"})
        statement = (
            select(SchedulerObservation)
            .where(
                SchedulerObservation.tenant_id == principal.tenant_id,
                SchedulerObservation.cluster_id == cluster_id,
                SchedulerObservation.source_identity == source_identity,
            )
            .order_by(SchedulerObservation.observed_at.desc())
            .limit(501)
        )
        with sessions() as session:
            rows = session.execute(statement).scalars().all()
            if not rows:
                raise HTTPException(status_code=404, detail={"code": "scheduler_attempt_not_found"})
            limited = len(rows) > 500
            timeline = derive_allocation_timeline(list(reversed(rows[:500])))
            coverage_reasons = list(timeline.coverage_reasons)
            if limited:
                coverage_reasons.append("observation_window_limited")
            return AllocationTimelineView(
                source_identity=timeline.source_identity,
                job_id=timeline.job_id,
                latest_state=timeline.latest_state,
                latest_observed_at=timeline.latest_observed_at,
                coverage_status=timeline.coverage_status,
                coverage_reasons=sorted(coverage_reasons),
                intervals=[asdict(interval) for interval in timeline.intervals],
            )

    @app.get("/v1/reports/{attempt_id}", response_model=ReportRevisionView)
    def report_revision(
        attempt_id: str = Path(
            min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$"
        ),
        cluster_id: str = Query(min_length=1, max_length=128),
        revision_number: Optional[int] = Query(default=None, ge=1, le=2**63 - 1),
        principal: MachinePrincipal = Depends(authenticator.authenticate),
    ) -> ReportRevisionView:
        if cluster_id not in principal.clusters:
            raise HTTPException(status_code=404, detail={"code": "cluster_not_found"})
        identity = (principal.tenant_id, cluster_id, attempt_id)
        with sessions() as session:
            generation = session.get(AttemptGeneration, identity)
            head = session.get(ReportHead, identity)
            if (
                generation is None
                or generation.deleted_at is not None
                or head is None
                or head.deletion_generation != generation.generation
            ):
                raise HTTPException(status_code=404, detail={"code": "report_not_found"})
            statement = select(ReportRevision).where(
                ReportRevision.tenant_id == principal.tenant_id,
                ReportRevision.cluster_id == cluster_id,
                ReportRevision.attempt_id == attempt_id,
                ReportRevision.deletion_generation == generation.generation,
            )
            if revision_number is None:
                statement = statement.where(ReportRevision.revision_id == head.revision_id)
            else:
                statement = statement.where(ReportRevision.revision_number == revision_number)
            revision = session.scalar(statement)
            if revision is None:
                raise HTTPException(status_code=404, detail={"code": "report_not_found"})
            return ReportRevisionView(
                revision_id=revision.revision_id,
                tenant_id=revision.tenant_id,
                cluster_id=revision.cluster_id,
                attempt_id=revision.attempt_id,
                deletion_generation=revision.deletion_generation,
                revision_number=revision.revision_number,
                input_fingerprint=revision.input_fingerprint,
                segment_count=revision.segment_count,
                record_count=revision.record_count,
                report_sha256=revision.report_sha256,
                builder_version=revision.builder_version,
                created_at=revision.created_at,
                current=revision.revision_id == head.revision_id,
            )

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
