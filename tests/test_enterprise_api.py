from __future__ import annotations

import base64
import hashlib
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from ranklens_enterprise.api import create_app
from ranklens_enterprise.database import (
    AttemptGeneration,
    ReportHead,
    ReportRevision,
    SchedulerObservation,
    assert_schema_ready,
    build_engine,
    build_session_factory,
    initialize_schema,
)
from ranklens_enterprise.settings import MachinePrincipal, Settings


class EnterpriseApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.database_url = f"sqlite:///{root / 'catalog.sqlite3'}"
        self.token = "test-token-with-at-least-24-characters"
        settings = Settings(
            database_url=self.database_url,
            object_root=root / "objects",
            machine_tokens={
                self.token: MachinePrincipal("tenant-a", frozenset({"cluster-a"}))
            },
            bootstrap_schema=True,
        )
        self.client_context = TestClient(create_app(settings))
        self.client = self.client_context.__enter__()

    def tearDown(self) -> None:
        self.client_context.__exit__(None, None, None)
        self.temporary.cleanup()

    @staticmethod
    def request_body() -> dict:
        payload = b'{"record":"one"}\n'
        return {
            "envelope_major": 2,
            "cluster_id": "cluster-a",
            "attempt_id": "attempt-1",
            "producer_id": "node-agent-1",
            "transport_epoch": "epoch-1",
            "stream_id": "rank-summary",
            "first_sequence": 0,
            "last_sequence": 0,
            "record_count": 1,
            "deletion_generation": 0,
            "compression": "identity",
            "content_type": "application/x-ndjson",
            "expanded_size_bytes": len(payload),
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
            "payload_base64": base64.b64encode(payload).decode("ascii"),
        }

    def test_health_is_public_but_receipts_require_machine_auth(self) -> None:
        health = self.client.get("/healthz")
        unauthorized = self.client.post("/v1/segments", json=self.request_body())

        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["status"], "ok")
        self.assertEqual(unauthorized.status_code, 401)
        self.assertEqual(unauthorized.headers["cache-control"], "no-store")
        self.assertIn("x-request-id", unauthorized.headers)

    def test_admission_replay_and_tenant_scoped_receipt_api(self) -> None:
        headers = {"authorization": f"Bearer {self.token}", "x-request-id": "request-1"}

        first = self.client.post("/v1/segments", json=self.request_body(), headers=headers)
        replay = self.client.post("/v1/segments", json=self.request_body(), headers=headers)

        self.assertEqual(first.status_code, 201)
        self.assertEqual(replay.status_code, 201)
        self.assertFalse(first.json()["replayed"])
        self.assertTrue(replay.json()["replayed"])
        self.assertEqual(first.json()["receipt_id"], replay.json()["receipt_id"])
        receipt = self.client.get(
            f"/v1/receipts/{first.json()['receipt_id']}", headers=headers
        )
        self.assertEqual(receipt.status_code, 200)
        self.assertEqual(receipt.json()["tenant_id"], "tenant-a")
        self.assertEqual(first.headers["x-request-id"], "request-1")

    def test_payload_cannot_select_a_tenant(self) -> None:
        body = self.request_body()
        body["tenant_id"] = "tenant-b"

        response = self.client.post(
            "/v1/segments",
            json=body,
            headers={"authorization": f"Bearer {self.token}"},
        )

        self.assertEqual(response.status_code, 422)

    def test_segment_endpoint_requires_json_and_a_bounded_body(self) -> None:
        headers = {"authorization": f"Bearer {self.token}"}
        wrong_type = self.client.post("/v1/segments", content=b"x", headers=headers)

        self.assertEqual(wrong_type.status_code, 415)
        self.assertEqual(wrong_type.json()["code"], "unsupported_content_type")

    def test_attempt_catalog_is_cluster_scoped_and_stably_paginated(self) -> None:
        headers = {"authorization": f"Bearer {self.token}"}
        first_body = self.request_body()
        first_body.update(
            {
                "attempt_id": "attempt-a",
                "workflow_execution_id": "workflow-1",
                "logical_case_id": "case-a",
                "scheduler_source_identity": "slurm:cluster-a:derived:a",
            }
        )
        second_body = self.request_body()
        second_body.update({"attempt_id": "attempt-b", "producer_id": "node-agent-2"})
        self.assertEqual(
            self.client.post("/v1/segments", json=first_body, headers=headers).status_code,
            201,
        )
        self.assertEqual(
            self.client.post("/v1/segments", json=second_body, headers=headers).status_code,
            201,
        )

        first_page = self.client.get(
            "/v1/attempts", params={"cluster_id": "cluster-a", "limit": 1}, headers=headers
        )
        self.assertEqual(first_page.status_code, 200)
        self.assertEqual([item["attempt_id"] for item in first_page.json()["items"]], ["attempt-a"])
        self.assertEqual(first_page.json()["next_cursor"], "attempt-a")
        second_page = self.client.get(
            "/v1/attempts",
            params={
                "cluster_id": "cluster-a",
                "limit": 1,
                "cursor": first_page.json()["next_cursor"],
            },
            headers=headers,
        )
        self.assertEqual([item["attempt_id"] for item in second_page.json()["items"]], ["attempt-b"])
        self.assertIsNone(second_page.json()["next_cursor"])

        detail = self.client.get(
            "/v1/attempts/attempt-a", params={"cluster_id": "cluster-a"}, headers=headers
        )
        denied = self.client.get(
            "/v1/attempts/attempt-a", params={"cluster_id": "cluster-b"}, headers=headers
        )
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["workflow_execution_id"], "workflow-1")
        self.assertEqual(detail.json()["scheduler_state"], "unknown")
        self.assertEqual(detail.json()["telemetry_status"], "available")
        self.assertEqual(detail.json()["segment_count"], 1)
        self.assertEqual(denied.status_code, 404)

    def test_production_schema_check_requires_migration_marker(self) -> None:
        engine = build_engine("sqlite:///:memory:")
        initialize_schema(engine)
        with self.assertRaisesRegex(RuntimeError, "alembic_version"):
            assert_schema_ready(engine)
        engine.dispose()

    def test_scheduler_observations_are_cluster_scoped(self) -> None:
        engine = build_engine(self.database_url)
        sessions = build_session_factory(engine)
        observed = datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc)
        with sessions() as session:
            session.add(
                SchedulerObservation(
                    observation_id="observation-1",
                    tenant_id="tenant-a",
                    cluster_id="cluster-a",
                    adapter="slurm",
                    adapter_version="slurm-sacct-json-v1",
                    source_identity="slurm:cluster-a:derived:abc",
                    job_id="44",
                    array_job_id=None,
                    array_task_id=None,
                    step_id=None,
                    restart_count=0,
                    state="running",
                    state_reason=None,
                    submitted_at=observed,
                    started_at=observed,
                    ended_at=None,
                    allocated_nodes=2,
                    allocated_cpus=64,
                    account="science",
                    partition="compute",
                    user_ref="opaque-user-ref",
                    observed_at=observed,
                    raw_json='{"job_id":"44"}',
                    first_seen_at=observed,
                )
            )
            session.commit()
        engine.dispose()

        headers = {"authorization": f"Bearer {self.token}"}
        allowed = self.client.get(
            "/v1/scheduler/observations?cluster_id=cluster-a&job_id=44",
            headers=headers,
        )
        denied = self.client.get(
            "/v1/scheduler/observations?cluster_id=cluster-b",
            headers=headers,
        )
        allocation = self.client.get(
            "/v1/scheduler/allocation",
            params={
                "cluster_id": "cluster-a",
                "source_identity": "slurm:cluster-a:derived:abc",
            },
            headers=headers,
        )

        self.assertEqual(allowed.status_code, 200)
        self.assertEqual(allowed.json()[0]["job_id"], "44")
        self.assertEqual(allowed.json()[0]["state"], "running")
        self.assertEqual(allowed.json()[0]["allocated_cpus"], 64)
        self.assertEqual(denied.status_code, 404)
        self.assertEqual(allocation.status_code, 200)
        self.assertEqual(allocation.json()["coverage_status"], "observed")
        self.assertEqual(allocation.json()["intervals"][0]["allocated_nodes"], 2)
        self.assertIn("attempt_not_terminal", allocation.json()["coverage_reasons"])

    def test_report_metadata_is_current_generation_and_cluster_scoped(self) -> None:
        engine = build_engine(self.database_url)
        sessions = build_session_factory(engine)
        now = datetime(2026, 9, 19, 12, 0, tzinfo=timezone.utc)
        with sessions.begin() as session:
            session.add(
                AttemptGeneration(
                    tenant_id="tenant-a",
                    cluster_id="cluster-a",
                    attempt_id="attempt-report",
                    generation=0,
                    deleted_at=None,
                )
            )
            session.add(
                ReportRevision(
                    revision_id="revision-1",
                    tenant_id="tenant-a",
                    cluster_id="cluster-a",
                    attempt_id="attempt-report",
                    deletion_generation=0,
                    revision_number=1,
                    input_fingerprint="1" * 64,
                    segment_count=2,
                    record_count=8,
                    report_object_key="internal/object.json",
                    report_sha256="2" * 64,
                    parquet_object_key="internal/records.parquet",
                    parquet_sha256="3" * 64,
                    parquet_schema_version="normalized-record-v1",
                    parquet_row_count=8,
                    builder_version="attempt-manifest-v1",
                    created_at=now,
                )
            )
            session.add(
                ReportHead(
                    tenant_id="tenant-a",
                    cluster_id="cluster-a",
                    attempt_id="attempt-report",
                    deletion_generation=0,
                    revision_number=1,
                    revision_id="revision-1",
                    input_fingerprint="1" * 64,
                    updated_at=now,
                )
            )
        engine.dispose()
        headers = {"authorization": f"Bearer {self.token}"}

        allowed = self.client.get(
            "/v1/reports/attempt-report",
            params={"cluster_id": "cluster-a"},
            headers=headers,
        )
        denied = self.client.get(
            "/v1/reports/attempt-report",
            params={"cluster_id": "cluster-b"},
            headers=headers,
        )

        self.assertEqual(allowed.status_code, 200)
        self.assertEqual(allowed.json()["revision_number"], 1)
        self.assertEqual(allowed.json()["record_count"], 8)
        self.assertTrue(allowed.json()["current"])
        self.assertFalse(allowed.json()["download_available"])
        self.assertTrue(allowed.json()["parquet_available"])
        self.assertEqual(allowed.json()["parquet_schema_version"], "normalized-record-v1")
        self.assertEqual(allowed.json()["parquet_row_count"], 8)
        self.assertNotIn("report_object_key", allowed.json())
        self.assertNotIn("parquet_object_key", allowed.json())
        self.assertEqual(denied.status_code, 404)

        engine = build_engine(self.database_url)
        sessions = build_session_factory(engine)
        with sessions.begin() as session:
            generation = session.get(
                AttemptGeneration, ("tenant-a", "cluster-a", "attempt-report")
            )
            assert generation is not None
            generation.generation = 1
            generation.deleted_at = now
        engine.dispose()
        deleted = self.client.get(
            "/v1/reports/attempt-report",
            params={"cluster_id": "cluster-a"},
            headers=headers,
        )
        self.assertEqual(deleted.status_code, 404)


if __name__ == "__main__":
    unittest.main()
