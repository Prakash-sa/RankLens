from __future__ import annotations

import base64
import hashlib
import io
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq
from sqlalchemy import func, select

from ranklens_enterprise.contracts import SegmentUpload
from ranklens_enterprise.database import (
    AdmissionReservation,
    AttemptDeletion,
    AttemptGeneration,
    AttemptHold,
    AttemptRecord,
    NormalizedSegment,
    ObjectGcCandidate,
    OutboxRecord,
    ReportHead,
    ReportRevision,
    RetentionPolicyBackfill,
    RetentionPolicyRevision,
    SegmentManifest,
    build_engine,
    build_session_factory,
    initialize_schema,
)
from ranklens_enterprise.ingestion import (
    AdmissionConflict,
    AdmissionError,
    DeletionHeldError,
    IngestionService,
)
from ranklens_enterprise.settings import MachinePrincipal
from ranklens_enterprise.storage import LocalObjectStore
from ranklens_enterprise.storage import ObjectIntegrityError
from ranklens_enterprise.worker import (
    AttemptDeletionWorker,
    ObjectGcAuditWorker,
    ObjectGcWorker,
    OutboxWorker,
    ReportRevisionWorker,
    ReservationSweeper,
    RetentionSweeper,
)


class EnterpriseIngestionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.engine = build_engine(f"sqlite:///{root / 'catalog.sqlite3'}")
        initialize_schema(self.engine)
        self.sessions = build_session_factory(self.engine)
        self.objects = LocalObjectStore(root / "objects")
        self.service = IngestionService(self.sessions, self.objects)
        self.principal = MachinePrincipal(
            "tenant-a", frozenset({"cluster-a"}), "agent-test"
        )

    def tearDown(self) -> None:
        self.engine.dispose()
        self.temporary.cleanup()

    @staticmethod
    def segment(
        payload: bytes = b'{"record":"one"}\n',
        *,
        first: int = 0,
        last: int = 0,
        record_count: int = 1,
        deletion_generation: int = 0,
        attempt_id: str = "attempt-1",
        workflow_execution_id: str | None = None,
        logical_case_id: str | None = None,
        scheduler_source_identity: str | None = None,
    ) -> SegmentUpload:
        return SegmentUpload(
            cluster_id="cluster-a",
            attempt_id=attempt_id,
            workflow_execution_id=workflow_execution_id,
            logical_case_id=logical_case_id,
            scheduler_source_identity=scheduler_source_identity,
            producer_id="node-agent-1",
            transport_epoch="epoch-1",
            stream_id="rank-summary",
            first_sequence=first,
            last_sequence=last,
            record_count=record_count,
            deletion_generation=deletion_generation,
            expanded_size_bytes=len(payload),
            payload_sha256=hashlib.sha256(payload).hexdigest(),
            payload_base64=base64.b64encode(payload).decode("ascii"),
        )

    def test_durable_admission_and_exact_replay_have_one_effect(self) -> None:
        segment = self.segment()

        first = self.service.admit(self.principal, segment)
        replay = self.service.admit(self.principal, segment)

        self.assertEqual(first.status, "DURABLE")
        self.assertFalse(first.replayed)
        self.assertTrue(replay.replayed)
        self.assertEqual(first.receipt_id, replay.receipt_id)
        self.assertTrue(self.objects.exists_verified(first.object_key, first.payload_sha256))
        with self.sessions() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(SegmentManifest)), 1)
            self.assertEqual(session.scalar(select(func.count()).select_from(OutboxRecord)), 1)
            manifest = session.scalar(select(SegmentManifest))
            reservation = session.scalar(select(AdmissionReservation))
            assert manifest is not None
            assert reservation is not None
            self.assertEqual(manifest.credential_id, "agent-test")
            self.assertEqual(reservation.credential_id, "agent-test")
        receipt = self.service.get_receipt("tenant-a", first.receipt_id)
        assert receipt is not None
        self.assertEqual(receipt.admitted_by_credential_id, "agent-test")

    def test_same_range_with_different_content_is_a_conflict(self) -> None:
        self.service.admit(self.principal, self.segment())

        with self.assertRaisesRegex(AdmissionConflict, "different content") as raised:
            self.service.admit(self.principal, self.segment(b'{"record":"changed"}\n'))

        self.assertEqual(raised.exception.code, "range_digest_conflict")

    def test_admission_creates_one_durable_attempt_identity_and_counts_once(self) -> None:
        identity = "slurm:cluster-a:derived:abc"
        first = self.segment(
            workflow_execution_id="workflow-1",
            logical_case_id="case-1",
            scheduler_source_identity=identity,
        )
        self.service.admit(self.principal, first)
        self.service.admit(self.principal, first)
        self.service.admit(
            self.principal,
            self.segment(
                b'{"record":"two"}\n',
                first=1,
                last=1,
                workflow_execution_id="workflow-1",
                logical_case_id="case-1",
                scheduler_source_identity=identity,
            ),
        )

        with self.sessions() as session:
            record = session.get(AttemptRecord, ("tenant-a", "cluster-a", "attempt-1"))
            assert record is not None
            self.assertEqual(record.workflow_execution_id, "workflow-1")
            self.assertEqual(record.logical_case_id, "case-1")
            self.assertEqual(record.scheduler_source_identity, identity)
            self.assertEqual(record.scheduler_state, "unknown")
            self.assertEqual(record.segment_count, 2)
            self.assertEqual(record.record_count, 2)

    def test_first_admission_persists_a_fixed_default_retention_deadline(self) -> None:
        service = IngestionService(
            self.sessions,
            self.objects,
            default_retention_seconds=3600,
        )
        service.admit(
            self.principal,
            self.segment(attempt_id="attempt-retained"),
        )
        with self.sessions() as session:
            attempt = session.get(
                AttemptRecord, ("tenant-a", "cluster-a", "attempt-retained")
            )
            assert attempt is not None
            assert attempt.retention_expires_at is not None
            first_deadline = attempt.retention_expires_at
            self.assertEqual(
                attempt.retention_expires_at - attempt.first_admitted_at,
                timedelta(seconds=3600),
            )

        service.admit(
            self.principal,
            self.segment(
                b'{"record":"two"}\n',
                first=1,
                last=1,
                attempt_id="attempt-retained",
            ),
        )
        with self.sessions() as session:
            attempt = session.get(
                AttemptRecord, ("tenant-a", "cluster-a", "attempt-retained")
            )
            assert attempt is not None
            self.assertEqual(attempt.retention_expires_at, first_deadline)
            self.assertIsNone(attempt.retention_policy_version)

    def test_append_only_cluster_policy_is_fixed_at_first_admission(self) -> None:
        first_policy = self.service.create_retention_policy_revision(
            "tenant-a", "cluster-a", 3600, "retention-admin-a"
        )
        self.assertEqual(first_policy.version, 1)
        first_segment = self.segment(attempt_id="attempt-policy-one").model_copy(
            update={"producer_id": "node-agent-policy-one"}
        )
        self.service.admit(self.principal, first_segment)

        second_policy = self.service.create_retention_policy_revision(
            "tenant-a", "cluster-a", 7200, "retention-admin-b"
        )
        self.assertEqual(second_policy.version, 2)
        self.service.admit(
            self.principal,
            self.segment(
                b'{"record":"later"}\n',
                first=1,
                last=1,
                attempt_id="attempt-policy-one",
            ).model_copy(update={"producer_id": "node-agent-policy-one"}),
        )
        self.service.admit(
            self.principal,
            self.segment(attempt_id="attempt-policy-two").model_copy(
                update={"producer_id": "node-agent-policy-two"}
            ),
        )
        disabled_policy = self.service.create_retention_policy_revision(
            "tenant-a", "cluster-a", 0, "retention-admin-c"
        )
        self.assertEqual(disabled_policy.version, 3)
        self.service.admit(
            self.principal,
            self.segment(attempt_id="attempt-policy-disabled").model_copy(
                update={"producer_id": "node-agent-policy-disabled"}
            ),
        )

        with self.sessions() as session:
            first = session.get(
                AttemptRecord, ("tenant-a", "cluster-a", "attempt-policy-one")
            )
            second = session.get(
                AttemptRecord, ("tenant-a", "cluster-a", "attempt-policy-two")
            )
            disabled = session.get(
                AttemptRecord,
                ("tenant-a", "cluster-a", "attempt-policy-disabled"),
            )
            assert first is not None
            assert second is not None
            assert disabled is not None
            assert first.retention_expires_at is not None
            assert second.retention_expires_at is not None
            self.assertEqual(first.retention_policy_version, 1)
            self.assertEqual(
                first.retention_expires_at - first.first_admitted_at,
                timedelta(seconds=3600),
            )
            self.assertEqual(second.retention_policy_version, 2)
            self.assertEqual(
                second.retention_expires_at - second.first_admitted_at,
                timedelta(seconds=7200),
            )
            self.assertEqual(disabled.retention_policy_version, 3)
            self.assertIsNone(disabled.retention_expires_at)
            revisions = list(
                session.scalars(
                    select(RetentionPolicyRevision).order_by(
                        RetentionPolicyRevision.version
                    )
                )
            )
            self.assertEqual([item.version for item in revisions], [1, 2, 3])
            self.assertEqual(
                [item.created_by_credential_id for item in revisions],
                ["retention-admin-a", "retention-admin-b", "retention-admin-c"],
            )

    def test_scheduled_policy_activates_by_version_without_rewriting_attempts(self) -> None:
        current = {"now": datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)}
        service = IngestionService(
            self.sessions,
            self.objects,
            clock=lambda: current["now"],
        )
        service.admit(
            self.principal,
            self.segment(attempt_id="legacy-before-schedule").model_copy(
                update={"producer_id": "node-legacy-before-schedule"}
            ),
        )
        first = service.create_retention_policy_revision(
            "tenant-a", "cluster-a", 3600, "retention-admin-a"
        )
        scheduled_at = current["now"] + timedelta(hours=2)
        second = service.create_retention_policy_revision(
            "tenant-a",
            "cluster-a",
            7200,
            "retention-admin-a",
            scheduled_at,
        )
        with self.assertRaisesRegex(ValueError, "not yet effective"):
            service.preview_retention_policy_backfill(
                "tenant-a", "cluster-a", second.version, 10
            )

        current["now"] += timedelta(hours=1)
        service.admit(
            self.principal,
            self.segment(attempt_id="before-activation").model_copy(
                update={"producer_id": "node-before-activation"}
            ),
        )
        current["now"] = scheduled_at
        service.admit(
            self.principal,
            self.segment(attempt_id="after-activation").model_copy(
                update={"producer_id": "node-after-activation"}
            ),
        )
        third = service.create_retention_policy_revision(
            "tenant-a", "cluster-a", 10800, "retention-admin-b"
        )
        service.admit(
            self.principal,
            self.segment(attempt_id="after-superseding-revision").model_copy(
                update={"producer_id": "node-after-superseding-revision"}
            ),
        )

        self.assertEqual(first.version, 1)
        self.assertEqual(second.version, 2)
        self.assertEqual(second.effective_at, scheduled_at)
        self.assertEqual(third.version, 3)
        with self.sessions() as session:
            legacy = session.get(
                AttemptRecord,
                ("tenant-a", "cluster-a", "legacy-before-schedule"),
            )
            before = session.get(
                AttemptRecord, ("tenant-a", "cluster-a", "before-activation")
            )
            after = session.get(
                AttemptRecord, ("tenant-a", "cluster-a", "after-activation")
            )
            superseding = session.get(
                AttemptRecord,
                ("tenant-a", "cluster-a", "after-superseding-revision"),
            )
            assert legacy is not None
            assert before is not None
            assert after is not None
            assert superseding is not None
            self.assertIsNone(legacy.retention_policy_version)
            self.assertEqual(before.retention_policy_version, 1)
            self.assertEqual(after.retention_policy_version, 2)
            self.assertEqual(superseding.retention_policy_version, 3)
            self.assertEqual(
                after.retention_expires_at - after.first_admitted_at,
                timedelta(seconds=7200),
            )

    def test_case_policy_overrides_cluster_default_and_bounds_legacy_backfill(self) -> None:
        current = {"now": datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)}
        service = IngestionService(
            self.sessions,
            self.objects,
            clock=lambda: current["now"],
        )
        legacy_cases = {
            "legacy-case-a": "case-a",
            "legacy-case-b": "case-b",
            "legacy-no-case": None,
        }
        for attempt_id, logical_case_id in legacy_cases.items():
            service.admit(
                self.principal,
                self.segment(
                    attempt_id=attempt_id,
                    logical_case_id=logical_case_id,
                ).model_copy(update={"producer_id": f"node-{attempt_id}"}),
            )

        cluster_policy = service.create_retention_policy_revision(
            "tenant-a", "cluster-a", 3600, "retention-admin-a"
        )
        case_policy = service.create_retention_policy_revision(
            "tenant-a",
            "cluster-a",
            7200,
            "retention-admin-a",
            logical_case_id="case-a",
        )
        for attempt_id, logical_case_id in {
            "new-case-a": "case-a",
            "new-case-b": "case-b",
            "new-no-case": None,
        }.items():
            service.admit(
                self.principal,
                self.segment(
                    attempt_id=attempt_id,
                    logical_case_id=logical_case_id,
                ).model_copy(update={"producer_id": f"node-{attempt_id}"}),
            )
        newer_cluster_policy = service.create_retention_policy_revision(
            "tenant-a", "cluster-a", 10800, "retention-admin-b"
        )
        for attempt_id, logical_case_id in {
            "latest-case-a": "case-a",
            "latest-case-b": "case-b",
        }.items():
            service.admit(
                self.principal,
                self.segment(
                    attempt_id=attempt_id,
                    logical_case_id=logical_case_id,
                ).model_copy(update={"producer_id": f"node-{attempt_id}"}),
            )

        preview = service.preview_retention_policy_backfill(
            "tenant-a", "cluster-a", case_policy.version, 10
        )
        self.assertEqual(preview.logical_case_id, "case-a")
        self.assertEqual(preview.eligible_attempts, 1)
        applied = service.apply_retention_policy_backfill(
            "tenant-a",
            "cluster-a",
            case_policy.version,
            10,
            expected_eligible_attempts=1,
            expected_due_attempts=0,
            requested_by_credential_id="retention-admin-a",
        )
        self.assertEqual(applied.updated_attempts, 1)

        expected_versions = {
            "legacy-case-a": case_policy.version,
            "legacy-case-b": None,
            "legacy-no-case": None,
            "new-case-a": case_policy.version,
            "new-case-b": cluster_policy.version,
            "new-no-case": cluster_policy.version,
            "latest-case-a": case_policy.version,
            "latest-case-b": newer_cluster_policy.version,
        }
        with self.sessions() as session:
            for attempt_id, expected_version in expected_versions.items():
                attempt = session.get(
                    AttemptRecord, ("tenant-a", "cluster-a", attempt_id)
                )
                assert attempt is not None
                self.assertEqual(
                    attempt.retention_policy_version,
                    expected_version,
                    attempt_id,
                )
            revisions = list(
                session.scalars(
                    select(RetentionPolicyRevision).order_by(
                        RetentionPolicyRevision.version
                    )
                )
            )
            self.assertEqual(
                [revision.logical_case_id for revision in revisions],
                [None, "case-a", None],
            )

    def test_reviewed_retention_backfill_is_bounded_audited_and_hold_aware(self) -> None:
        now = datetime.now(timezone.utc)
        for attempt_id in ("legacy-due", "legacy-held", "legacy-future", "legacy-deleting"):
            self.service.admit(
                self.principal,
                self.segment(attempt_id=attempt_id).model_copy(
                    update={"producer_id": f"node-{attempt_id}"}
                ),
            )
        hold_id = "00000000-0000-0000-0000-000000000091"
        self.service.place_attempt_hold(
            "tenant-a",
            "cluster-a",
            "legacy-held",
            hold_id,
            "preserve investigation evidence",
        )
        self.service.delete_attempt("tenant-a", "cluster-a", "legacy-deleting")
        with self.sessions.begin() as session:
            for attempt_id in ("legacy-due", "legacy-held", "legacy-deleting"):
                attempt = session.get(
                    AttemptRecord, ("tenant-a", "cluster-a", attempt_id)
                )
                assert attempt is not None
                attempt.first_admitted_at = now - timedelta(hours=2)
            future = session.get(
                AttemptRecord, ("tenant-a", "cluster-a", "legacy-future")
            )
            assert future is not None
            future.first_admitted_at = now

        policy = self.service.create_retention_policy_revision(
            "tenant-a", "cluster-a", 3600, "retention-admin-a"
        )
        with self.assertRaisesRegex(ValueError, "exceed max_attempts"):
            self.service.preview_retention_policy_backfill(
                "tenant-a", "cluster-a", policy.version, 2
            )
        preview = self.service.preview_retention_policy_backfill(
            "tenant-a", "cluster-a", policy.version, 10
        )
        self.assertEqual(preview.eligible_attempts, 3)
        self.assertEqual(preview.due_attempts, 2)
        self.assertEqual(preview.updated_attempts, 0)
        self.assertIsNone(preview.backfill_id)

        with self.assertRaisesRegex(ValueError, "preview is stale"):
            self.service.apply_retention_policy_backfill(
                "tenant-a",
                "cluster-a",
                policy.version,
                10,
                expected_eligible_attempts=4,
                expected_due_attempts=2,
                requested_by_credential_id="retention-admin-a",
            )
        with self.sessions() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(RetentionPolicyBackfill)),
                0,
            )
            self.assertEqual(
                session.scalar(
                    select(func.count())
                    .select_from(AttemptRecord)
                    .where(AttemptRecord.retention_policy_version.is_not(None))
                ),
                0,
            )

        applied = self.service.apply_retention_policy_backfill(
            "tenant-a",
            "cluster-a",
            policy.version,
            10,
            expected_eligible_attempts=preview.eligible_attempts,
            expected_due_attempts=preview.due_attempts,
            requested_by_credential_id="retention-admin-a",
        )
        self.assertEqual(applied.updated_attempts, 3)
        self.assertIsNotNone(applied.backfill_id)
        self.assertEqual(
            self.service.preview_retention_policy_backfill(
                "tenant-a", "cluster-a", policy.version, 10
            ).eligible_attempts,
            0,
        )
        with self.sessions() as session:
            audit = session.get(RetentionPolicyBackfill, applied.backfill_id)
            assert audit is not None
            self.assertEqual(audit.requested_by_credential_id, "retention-admin-a")
            self.assertEqual(audit.updated_attempts, 3)
            deleting = session.get(
                AttemptRecord, ("tenant-a", "cluster-a", "legacy-deleting")
            )
            assert deleting is not None
            self.assertIsNone(deleting.retention_policy_version)
            future = session.get(
                AttemptRecord, ("tenant-a", "cluster-a", "legacy-future")
            )
            assert future is not None
            self.assertEqual(future.retention_policy_version, policy.version)
            self.assertEqual(
                future.retention_expires_at - future.first_admitted_at,
                timedelta(seconds=3600),
            )

        sweeper = RetentionSweeper(
            self.sessions,
            self.service,
            batch_size=10,
            clock=lambda: now,
        )
        self.assertEqual(sweeper.run_once(), {"queued": 1, "held": 1})
        with self.sessions() as session:
            self.assertEqual(
                session.get(
                    AttemptDeletion, ("tenant-a", "cluster-a", "legacy-due")
                ).state,
                "pending",
            )
            self.assertEqual(
                session.get(
                    AttemptDeletion, ("tenant-a", "cluster-a", "legacy-held")
                ).state,
                "held",
            )

    def test_admission_rejects_conflicting_attempt_binding(self) -> None:
        self.service.admit(
            self.principal,
            self.segment(scheduler_source_identity="slurm:cluster-a:derived:first"),
        )

        with self.assertRaises(AdmissionConflict) as raised:
            self.service.admit(
                self.principal,
                self.segment(
                    b'{"record":"two"}\n',
                    first=1,
                    last=1,
                    scheduler_source_identity="slurm:cluster-a:derived:second",
                ),
            )

        self.assertEqual(raised.exception.code, "attempt_identity_conflict")
        with self.sessions() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(SegmentManifest)), 1)

    def test_retry_replaces_terminal_reservation_with_a_new_fenced_identity(self) -> None:
        segment = self.segment()
        old_id = "00000000-0000-0000-0000-000000000001"
        with self.sessions.begin() as session:
            session.add(
                AdmissionReservation(
                    reservation_id=old_id,
                    tenant_id="tenant-a",
                    cluster_id="cluster-a",
                    attempt_id="attempt-1",
                    producer_id="node-agent-1",
                    transport_epoch="epoch-1",
                    stream_id="rank-summary",
                    first_sequence=0,
                    last_sequence=0,
                    payload_sha256="f" * 64,
                    object_key="stale/object.segment",
                    deletion_generation=0,
                    state="expired",
                    created_at=datetime(2026, 9, 20, tzinfo=timezone.utc),
                )
            )

        receipt = self.service.admit(self.principal, segment)

        self.assertEqual(receipt.status, "DURABLE")
        with self.sessions() as session:
            reservation = session.scalar(select(AdmissionReservation))
            assert reservation is not None
            self.assertNotEqual(reservation.reservation_id, old_id)
            self.assertEqual(reservation.state, "committed")
            self.assertEqual(reservation.payload_sha256, segment.payload_sha256)

    def test_reservation_sweeper_expires_only_abandoned_bounded_rows(self) -> None:
        now = datetime(2026, 9, 21, 12, 0, tzinfo=timezone.utc)
        common = {
            "tenant_id": "tenant-a",
            "cluster_id": "cluster-a",
            "attempt_id": "attempt-1",
            "producer_id": "node-agent-1",
            "transport_epoch": "epoch-1",
            "stream_id": "rank-summary",
            "payload_sha256": "a" * 64,
            "object_key": "pending/object.segment",
            "deletion_generation": 0,
        }
        with self.sessions.begin() as session:
            session.add_all(
                [
                    AdmissionReservation(
                        reservation_id="00000000-0000-0000-0000-000000000011",
                        first_sequence=0,
                        last_sequence=0,
                        state="reserved",
                        created_at=now - timedelta(minutes=20),
                        **common,
                    ),
                    AdmissionReservation(
                        reservation_id="00000000-0000-0000-0000-000000000012",
                        first_sequence=1,
                        last_sequence=1,
                        state="reserved",
                        created_at=now - timedelta(minutes=5),
                        **common,
                    ),
                    AdmissionReservation(
                        reservation_id="00000000-0000-0000-0000-000000000013",
                        first_sequence=2,
                        last_sequence=2,
                        state="committed",
                        created_at=now - timedelta(minutes=20),
                        **common,
                    ),
                ]
            )
        sweeper = ReservationSweeper(
            self.sessions,
            ttl_seconds=900,
            batch_size=1,
            clock=lambda: now,
        )

        self.assertEqual(sweeper.run_once(), 1)
        self.assertEqual(sweeper.run_once(), 0)

        with self.sessions() as session:
            rows = list(
                session.scalars(
                    select(AdmissionReservation).order_by(
                        AdmissionReservation.reservation_id
                    )
                )
            )
            self.assertEqual([row.state for row in rows], ["expired", "reserved", "committed"])
            candidates = list(session.scalars(select(ObjectGcCandidate)))
            self.assertEqual(len(candidates), 1)
            self.assertEqual(candidates[0].object_key, "pending/object.segment")

    def test_orphan_gc_deletes_only_after_expiry_and_grace_period(self) -> None:
        now = datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc)
        payload = b"orphaned upload"
        digest = hashlib.sha256(payload).hexdigest()
        object_key = "v2/orphan.segment"
        self.objects.put_verified(object_key, payload, digest)
        with self.sessions.begin() as session:
            session.add(
                AdmissionReservation(
                    reservation_id="00000000-0000-0000-0000-000000000021",
                    tenant_id="tenant-a",
                    cluster_id="cluster-a",
                    attempt_id="attempt-1",
                    producer_id="node-agent-1",
                    transport_epoch="epoch-1",
                    stream_id="rank-summary",
                    first_sequence=0,
                    last_sequence=0,
                    payload_sha256=digest,
                    object_key=object_key,
                    deletion_generation=0,
                    state="reserved",
                    created_at=now - timedelta(minutes=20),
                )
            )
        sweeper = ReservationSweeper(
            self.sessions, ttl_seconds=900, gc_grace_seconds=300, clock=lambda: now
        )
        self.assertEqual(sweeper.run_once(), 1)
        early_gc = ObjectGcWorker(self.sessions, self.objects, clock=lambda: now)
        self.assertIsNone(early_gc.run_once())

        gc = ObjectGcWorker(
            self.sessions,
            self.objects,
            clock=lambda: now + timedelta(seconds=301),
        )
        self.assertEqual(gc.run_once(), "deleted")
        self.assertFalse(self.objects.exists_verified(object_key, digest))
        with self.sessions() as session:
            candidate = session.scalar(select(ObjectGcCandidate))
            assert candidate is not None
            self.assertEqual(candidate.state, "deleted")
            self.assertIsNotNone(candidate.completed_at)

    def test_orphan_gc_preserves_object_when_delayed_commit_is_visible(self) -> None:
        now = datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc)
        payload = b"delayed committed upload"
        digest = hashlib.sha256(payload).hexdigest()
        object_key = "v2/delayed.segment"
        self.objects.put_verified(object_key, payload, digest)
        with self.sessions.begin() as session:
            session.add(
                AdmissionReservation(
                    reservation_id="00000000-0000-0000-0000-000000000022",
                    tenant_id="tenant-a",
                    cluster_id="cluster-a",
                    attempt_id="attempt-1",
                    producer_id="node-agent-1",
                    transport_epoch="epoch-1",
                    stream_id="rank-summary",
                    first_sequence=0,
                    last_sequence=0,
                    payload_sha256=digest,
                    object_key=object_key,
                    deletion_generation=0,
                    state="reserved",
                    created_at=now - timedelta(minutes=20),
                )
            )
        sweeper = ReservationSweeper(
            self.sessions, ttl_seconds=900, gc_grace_seconds=300, clock=lambda: now
        )
        self.assertEqual(sweeper.run_once(), 1)
        with self.sessions.begin() as session:
            session.add(
                SegmentManifest(
                    receipt_id="00000000-0000-0000-0000-000000000023",
                    tenant_id="tenant-a",
                    cluster_id="cluster-a",
                    attempt_id="attempt-1",
                    producer_id="node-agent-1",
                    transport_epoch="epoch-1",
                    stream_id="rank-summary",
                    first_sequence=0,
                    last_sequence=0,
                    record_count=1,
                    payload_sha256=digest,
                    object_key=object_key,
                    deletion_generation=0,
                    committed_at=now,
                )
            )
        gc = ObjectGcWorker(
            self.sessions,
            self.objects,
            clock=lambda: now + timedelta(seconds=301),
        )

        self.assertEqual(gc.run_once(), "protected")
        self.assertTrue(self.objects.exists_verified(object_key, digest))

    def test_orphan_gc_retries_integrity_failures_then_quarantines_candidate(self) -> None:
        now = [datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc)]
        with self.sessions.begin() as session:
            session.add(
                ObjectGcCandidate(
                    candidate_id="00000000-0000-0000-0000-000000000031",
                    object_key="v2/corrupt.segment",
                    payload_sha256="a" * 64,
                    state="pending",
                    not_before=now[0],
                    attempts=0,
                    last_error=None,
                    created_at=now[0],
                    completed_at=None,
                )
            )

        class FailingDeleteStore:
            def delete_verified(self, object_key: str, sha256: str) -> bool:
                raise ObjectIntegrityError("provider integrity failure")

        worker = ObjectGcWorker(self.sessions, FailingDeleteStore(), clock=lambda: now[0])
        for attempt in range(1, 6):
            self.assertEqual(worker.run_once(), "failed" if attempt == 5 else "retry")
            now[0] += timedelta(hours=2)

        with self.sessions() as session:
            candidate = session.get(
                ObjectGcCandidate, "00000000-0000-0000-0000-000000000031"
            )
            assert candidate is not None
            self.assertEqual(candidate.state, "failed")
            self.assertEqual(candidate.attempts, 5)
            self.assertIn("provider integrity failure", candidate.last_error or "")
            self.assertIsNotNone(candidate.completed_at)

    def test_object_gc_audit_verifies_referenced_report_artifacts(self) -> None:
        self.service.admit(self.principal, self.segment())
        self.assertTrue(OutboxWorker(self.sessions, self.objects).run_once())
        self.assertTrue(ReportRevisionWorker(self.sessions, self.objects).run_once())
        now = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
        with self.sessions.begin() as session:
            candidates = list(session.scalars(select(ObjectGcCandidate)))
            self.assertEqual(len(candidates), 2)
            for candidate in candidates:
                candidate.not_before = now

        audit = ObjectGcAuditWorker(
            self.sessions,
            self.objects,
            audit_interval_seconds=600,
            clock=lambda: now,
        )
        self.assertEqual(audit.run_once(), {"protected": 2})
        with self.sessions() as session:
            candidates = list(session.scalars(select(ObjectGcCandidate)))
            self.assertTrue(
                all(
                    candidate.not_before
                    == (now + timedelta(seconds=600)).replace(tzinfo=None)
                    for candidate in candidates
                )
            )

        missing = next(
            candidate for candidate in candidates if candidate.object_key.endswith(".json")
        )
        self.assertTrue(
            self.objects.delete_verified(missing.object_key, missing.payload_sha256)
        )
        with self.sessions.begin() as session:
            candidate = session.get(ObjectGcCandidate, missing.candidate_id)
            assert candidate is not None
            candidate.not_before = now

        self.assertEqual(audit.run_once(), {"failed": 1})
        with self.sessions() as session:
            candidate = session.get(ObjectGcCandidate, missing.candidate_id)
            assert candidate is not None
            self.assertEqual(candidate.state, "failed")
            self.assertIn("referenced object missing", candidate.last_error or "")

    def test_object_gc_audit_releases_repaired_unreferenced_quarantine(self) -> None:
        now = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
        payload = b"repaired orphan"
        digest = hashlib.sha256(payload).hexdigest()
        object_key = "v2/repaired-orphan.segment"
        self.objects.put_verified(object_key, payload, digest)
        with self.sessions.begin() as session:
            session.add(
                ObjectGcCandidate(
                    candidate_id="00000000-0000-0000-0000-000000000051",
                    object_key=object_key,
                    payload_sha256=digest,
                    state="failed",
                    not_before=now,
                    attempts=5,
                    last_error="previous provider failure",
                    created_at=now - timedelta(days=1),
                    completed_at=now,
                )
            )

        audit = ObjectGcAuditWorker(
            self.sessions,
            self.objects,
            gc_grace_seconds=300,
            audit_interval_seconds=600,
            clock=lambda: now,
        )
        self.assertEqual(audit.run_once(), {})
        after_cooldown = now + timedelta(seconds=601)
        audit = ObjectGcAuditWorker(
            self.sessions,
            self.objects,
            gc_grace_seconds=300,
            audit_interval_seconds=600,
            clock=lambda: after_cooldown,
        )
        self.assertEqual(audit.run_once(), {"pending": 1})
        with self.sessions() as session:
            candidate = session.get(
                ObjectGcCandidate, "00000000-0000-0000-0000-000000000051"
            )
            assert candidate is not None
            self.assertEqual(candidate.attempts, 0)
            self.assertEqual(
                candidate.not_before,
                (after_cooldown + timedelta(seconds=300)).replace(tzinfo=None),
            )
            self.assertIsNone(candidate.completed_at)

        gc = ObjectGcWorker(
            self.sessions,
            self.objects,
            clock=lambda: after_cooldown + timedelta(seconds=301),
        )
        self.assertEqual(gc.run_once(), "deleted")
        self.assertFalse(self.objects.exists_verified(object_key, digest))

    def test_partial_sequence_overlap_is_rejected(self) -> None:
        eleven_records = b"".join(
            f'{{"record":{index}}}\n'.encode("ascii") for index in range(11)
        )
        two_records = b'{"record":20}\n{"record":21}\n'
        self.service.admit(
            self.principal,
            self.segment(eleven_records, first=10, last=20, record_count=11),
        )

        with self.assertRaises(AdmissionConflict) as raised:
            self.service.admit(
                self.principal,
                self.segment(two_records, first=20, last=21, record_count=2),
            )

        self.assertEqual(raised.exception.code, "sequence_overlap")

    def test_cluster_scope_is_derived_from_machine_principal(self) -> None:
        forbidden = MachinePrincipal("tenant-a", frozenset({"cluster-b"}))

        with self.assertRaises(AdmissionError) as raised:
            self.service.admit(forbidden, self.segment())

        self.assertEqual(raised.exception.status_code, 403)
        self.assertEqual(raised.exception.code, "cluster_forbidden")

    def test_deletion_fences_late_segments(self) -> None:
        self.service.admit(self.principal, self.segment())
        generation = self.service.delete_attempt("tenant-a", "cluster-a", "attempt-1")
        replayed_generation = self.service.delete_attempt(
            "tenant-a", "cluster-a", "attempt-1"
        )

        with self.assertRaises(AdmissionConflict) as raised:
            self.service.admit(self.principal, self.segment(first=1, last=1))

        self.assertEqual(generation, 1)
        self.assertEqual(replayed_generation, 1)
        self.assertEqual(raised.exception.code, "attempt_deleted")
        with self.sessions() as session:
            deletion = session.get(
                AttemptDeletion, ("tenant-a", "cluster-a", "attempt-1")
            )
            self.assertIsNotNone(deletion)
            assert deletion is not None
            self.assertEqual(deletion.deletion_generation, 1)
            self.assertEqual(deletion.state, "pending")
            self.assertEqual(deletion.attempts, 0)

    def test_retention_holds_block_deletion_until_every_hold_is_released(self) -> None:
        receipt = self.service.admit(self.principal, self.segment())
        first_hold = "00000000-0000-0000-0000-000000000061"
        second_hold = "00000000-0000-0000-0000-000000000062"
        self.service.place_attempt_hold(
            "tenant-a",
            "cluster-a",
            "attempt-1",
            first_hold,
            "legal review",
            placed_by_credential_id="retention-admin-a",
        )
        self.service.place_attempt_hold(
            "tenant-a",
            "cluster-a",
            "attempt-1",
            first_hold,
            "legal review",
            placed_by_credential_id="retention-admin-b",
        )
        self.service.place_attempt_hold(
            "tenant-a", "cluster-a", "attempt-1", second_hold, "customer request"
        )

        with self.assertRaises(DeletionHeldError) as raised:
            self.service.delete_attempt("tenant-a", "cluster-a", "attempt-1")
        self.assertEqual(raised.exception.hold_ids, (first_hold, second_hold))
        with self.sessions() as session:
            generation = session.get(
                AttemptGeneration, ("tenant-a", "cluster-a", "attempt-1")
            )
            deletion = session.get(
                AttemptDeletion, ("tenant-a", "cluster-a", "attempt-1")
            )
            assert generation is not None
            assert deletion is not None
            self.assertIsNone(generation.deleted_at)
            self.assertEqual(generation.generation, 0)
            self.assertEqual(deletion.state, "held")
            self.assertEqual(
                session.scalar(select(func.count()).select_from(AttemptHold)), 2
            )
            first = session.get(AttemptHold, first_hold)
            assert first is not None
            self.assertEqual(first.placed_by_credential_id, "retention-admin-a")
            self.assertIsNone(first.released_by_credential_id)
        self.assertTrue(self.objects.exists_verified(receipt.object_key, receipt.payload_sha256))

        self.assertTrue(
            self.service.release_attempt_hold(
                first_hold, released_by_credential_id="retention-admin-b"
            )
        )
        with self.sessions() as session:
            deletion = session.get(
                AttemptDeletion, ("tenant-a", "cluster-a", "attempt-1")
            )
            assert deletion is not None
            self.assertEqual(deletion.state, "held")
            first = session.get(AttemptHold, first_hold)
            assert first is not None
            self.assertEqual(first.released_by_credential_id, "retention-admin-b")

        self.assertTrue(self.service.release_attempt_hold(second_hold))
        self.assertFalse(self.service.release_attempt_hold(second_hold))
        with self.sessions() as session:
            generation = session.get(
                AttemptGeneration, ("tenant-a", "cluster-a", "attempt-1")
            )
            deletion = session.get(
                AttemptDeletion, ("tenant-a", "cluster-a", "attempt-1")
            )
            assert generation is not None
            assert deletion is not None
            self.assertEqual(generation.generation, 1)
            self.assertIsNotNone(generation.deleted_at)
            self.assertEqual(deletion.deletion_generation, 1)
            self.assertEqual(deletion.state, "pending")

        worker = AttemptDeletionWorker(self.sessions, self.objects, owner="deletion-a")
        self.assertTrue(worker.run_once())
        self.assertFalse(self.objects.exists_verified(receipt.object_key, receipt.payload_sha256))

    def test_retention_sweeper_queues_due_attempts_and_preserves_holds(self) -> None:
        now = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)
        self.service.admit(
            self.principal,
            self.segment(
                b'{"attempt":"due"}\n', attempt_id="attempt-due"
            ).model_copy(update={"producer_id": "node-agent-due"}),
        )
        self.service.admit(
            self.principal,
            self.segment(
                b'{"attempt":"held"}\n', attempt_id="attempt-held"
            ).model_copy(update={"producer_id": "node-agent-held"}),
        )
        self.service.admit(
            self.principal,
            self.segment(
                b'{"attempt":"future"}\n', attempt_id="attempt-future"
            ).model_copy(update={"producer_id": "node-agent-future"}),
        )
        hold_id = "00000000-0000-0000-0000-000000000081"
        self.service.place_attempt_hold(
            "tenant-a", "cluster-a", "attempt-held", hold_id, "retention exception"
        )
        with self.sessions.begin() as session:
            for attempt_id in ("attempt-due", "attempt-held"):
                record = session.get(
                    AttemptRecord, ("tenant-a", "cluster-a", attempt_id)
                )
                assert record is not None
                record.retention_expires_at = now - timedelta(seconds=1)
            future = session.get(
                AttemptRecord, ("tenant-a", "cluster-a", "attempt-future")
            )
            assert future is not None
            future.retention_expires_at = now + timedelta(hours=1)

        sweeper = RetentionSweeper(
            self.sessions,
            self.service,
            batch_size=10,
            clock=lambda: now,
        )
        self.assertEqual(sweeper.run_once(), {"queued": 1, "held": 1})
        self.assertEqual(sweeper.run_once(), {})
        with self.sessions() as session:
            due = session.get(
                AttemptDeletion, ("tenant-a", "cluster-a", "attempt-due")
            )
            held = session.get(
                AttemptDeletion, ("tenant-a", "cluster-a", "attempt-held")
            )
            future = session.get(
                AttemptDeletion, ("tenant-a", "cluster-a", "attempt-future")
            )
            assert due is not None
            assert held is not None
            self.assertEqual(due.state, "pending")
            self.assertEqual(held.state, "held")
            self.assertIsNone(future)

        deletions = AttemptDeletionWorker(self.sessions, self.objects, owner="deletion-a")
        self.assertTrue(deletions.run_once())
        self.assertTrue(self.service.release_attempt_hold(hold_id))
        self.assertTrue(deletions.run_once())
        with self.sessions() as session:
            self.assertIsNone(
                session.get(AttemptRecord, ("tenant-a", "cluster-a", "attempt-due"))
            )
            self.assertIsNone(
                session.get(AttemptRecord, ("tenant-a", "cluster-a", "attempt-held"))
            )
            self.assertIsNotNone(
                session.get(AttemptRecord, ("tenant-a", "cluster-a", "attempt-future"))
            )

    def test_receipt_lookup_is_tenant_scoped(self) -> None:
        receipt = self.service.admit(self.principal, self.segment())

        self.assertIsNotNone(self.service.get_receipt("tenant-a", receipt.receipt_id))
        self.assertIsNone(self.service.get_receipt("tenant-b", receipt.receipt_id))

    def test_payload_digest_and_expanded_size_are_verified(self) -> None:
        segment = self.segment().model_copy(update={"payload_sha256": "0" * 64})
        with self.assertRaises(AdmissionError) as digest_error:
            self.service.admit(self.principal, segment)
        self.assertEqual(digest_error.exception.code, "payload_digest_mismatch")

        segment = self.segment().model_copy(update={"expanded_size_bytes": 1})
        with self.assertRaises(AdmissionError) as size_error:
            self.service.admit(self.principal, segment)
        self.assertEqual(size_error.exception.code, "expanded_size_mismatch")

    def test_ndjson_shape_and_record_count_are_validated_before_receipt(self) -> None:
        invalid = self.segment(b"not-json\n")
        with self.assertRaises(AdmissionError) as invalid_error:
            self.service.admit(self.principal, invalid)
        self.assertEqual(invalid_error.exception.code, "invalid_ndjson")

        wrong_count = self.segment(
            b'{"record":1}\n{"record":2}\n', last=2, record_count=1
        )
        with self.assertRaises(AdmissionError) as count_error:
            self.service.admit(self.principal, wrong_count)
        self.assertEqual(count_error.exception.code, "record_count_mismatch")

    def test_outbox_worker_has_one_fenced_normalization_effect(self) -> None:
        self.service.admit(self.principal, self.segment())
        worker = OutboxWorker(self.sessions, self.objects, owner="worker-a")

        lease = worker.claim()
        self.assertIsNotNone(lease)
        assert lease is not None
        self.assertTrue(worker.execute(lease))
        self.assertFalse(worker.execute(lease))

        with self.sessions() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(NormalizedSegment)), 1)
            outbox = session.scalar(select(OutboxRecord))
            self.assertEqual(outbox.state, "completed")

    def test_deletion_generation_suppresses_stale_worker_publication(self) -> None:
        self.service.admit(self.principal, self.segment())
        worker = OutboxWorker(self.sessions, self.objects, owner="worker-a")
        lease = worker.claim()
        assert lease is not None
        self.service.delete_attempt("tenant-a", "cluster-a", "attempt-1")

        self.assertFalse(worker.execute(lease))

        with self.sessions() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(NormalizedSegment)), 0)
            outbox = session.scalar(select(OutboxRecord))
            self.assertEqual(outbox.state, "suppressed")

    def test_late_normalized_evidence_publishes_a_new_immutable_report_revision(self) -> None:
        first_receipt = self.service.admit(self.principal, self.segment())
        normalizer = OutboxWorker(self.sessions, self.objects, owner="normalizer-a")
        reports = ReportRevisionWorker(self.sessions, self.objects, owner="reporter-a")

        self.assertTrue(normalizer.run_once())
        first_report_lease = reports.claim()
        assert first_report_lease is not None
        self.assertTrue(reports.execute(first_report_lease))

        second_segment = self.segment(
            b'{"record":"two"}\n', first=1, last=1, record_count=1
        )
        second_receipt = self.service.admit(self.principal, second_segment)
        self.assertTrue(normalizer.run_once())
        second_report_lease = reports.claim()
        assert second_report_lease is not None
        self.assertTrue(reports.execute(second_report_lease))

        with self.sessions() as session:
            revisions = list(
                session.scalars(select(ReportRevision).order_by(ReportRevision.revision_number))
            )
            head = session.get(ReportHead, ("tenant-a", "cluster-a", "attempt-1"))
        self.assertEqual([item.revision_number for item in revisions], [1, 2])
        self.assertEqual([item.segment_count for item in revisions], [1, 2])
        self.assertEqual([item.record_count for item in revisions], [1, 2])
        self.assertIsNotNone(head)
        assert head is not None
        self.assertEqual(head.revision_id, revisions[1].revision_id)
        self.assertTrue(
            self.objects.exists_verified(
                revisions[0].report_object_key, revisions[0].report_sha256
            )
        )
        self.assertEqual(revisions[1].parquet_schema_version, "normalized-record-v2")
        self.assertEqual(revisions[1].parquet_row_count, 2)
        assert revisions[1].parquet_object_key is not None
        assert revisions[1].parquet_sha256 is not None
        parquet_payload = self.objects.read_verified(
            revisions[1].parquet_object_key, revisions[1].parquet_sha256
        )
        parquet_table = pq.read_table(io.BytesIO(parquet_payload))
        self.assertEqual(parquet_table.num_rows, 2)
        self.assertEqual(
            parquet_table.column("record_json").to_pylist(),
            ['{"record":"one"}', '{"record":"two"}'],
        )
        report = self.objects.read_verified(
            revisions[1].report_object_key, revisions[1].report_sha256
        )
        self.assertIn(first_receipt.receipt_id.encode("ascii"), report)
        self.assertIn(second_receipt.receipt_id.encode("ascii"), report)
        self.assertIn(revisions[1].parquet_sha256.encode("ascii"), report)

    def test_orphan_gc_preserves_published_report_artifacts(self) -> None:
        self.service.admit(self.principal, self.segment())
        self.assertTrue(OutboxWorker(self.sessions, self.objects).run_once())
        self.assertTrue(ReportRevisionWorker(self.sessions, self.objects).run_once())
        now = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)

        with self.sessions.begin() as session:
            revision = session.scalar(select(ReportRevision))
            assert revision is not None
            assert revision.parquet_object_key is not None
            assert revision.parquet_sha256 is not None
            artifacts = (
                (revision.report_object_key, revision.report_sha256),
                (revision.parquet_object_key, revision.parquet_sha256),
            )
            candidates = list(session.scalars(select(ObjectGcCandidate)))
            self.assertEqual(len(candidates), 2)
            self.assertEqual({candidate.state for candidate in candidates}, {"protected"})
            for candidate in candidates:
                candidate.state = "pending"
                candidate.not_before = now
                candidate.completed_at = None

        gc = ObjectGcWorker(self.sessions, self.objects, clock=lambda: now)
        self.assertEqual(gc.run_once(), "protected")
        self.assertEqual(gc.run_once(), "protected")
        for object_key, digest in artifacts:
            self.assertTrue(self.objects.exists_verified(object_key, digest))

    def test_failed_report_publication_leaves_reclaimable_artifact_reservations(self) -> None:
        self.service.admit(self.principal, self.segment())
        self.assertTrue(OutboxWorker(self.sessions, self.objects).run_once())

        class FailingPutStore:
            def read_verified(inner_self, object_key: str, digest: str) -> bytes:
                return self.objects.read_verified(object_key, digest)

            def put_verified(
                inner_self, object_key: str, payload: bytes, digest: str
            ) -> None:
                if object_key.endswith(".json"):
                    raise ObjectIntegrityError("simulated report object write failure")
                self.objects.put_verified(object_key, payload, digest)

        failing = ReportRevisionWorker(
            self.sessions,
            FailingPutStore(),
            owner="reporter-failing",
            orphan_gc_grace_seconds=300,
        )
        lease = failing.claim()
        assert lease is not None
        self.assertFalse(failing.execute(lease))

        with self.sessions() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(ReportRevision)), 0)
            self.assertEqual(session.scalar(select(func.count()).select_from(ReportHead)), 0)
            candidates = list(session.scalars(select(ObjectGcCandidate)))
            self.assertEqual(len(candidates), 2)
            self.assertEqual({candidate.state for candidate in candidates}, {"pending"})
            parquet_candidate = next(
                candidate
                for candidate in candidates
                if candidate.object_key.endswith(".parquet")
            )
            self.assertTrue(
                self.objects.exists_verified(
                    parquet_candidate.object_key, parquet_candidate.payload_sha256
                )
            )
            report_outbox = session.get(OutboxRecord, lease.outbox_id)
            assert report_outbox is not None
            self.assertEqual(report_outbox.state, "pending")

        retry = ReportRevisionWorker(
            self.sessions,
            self.objects,
            owner="reporter-retry",
            orphan_gc_grace_seconds=300,
        )
        self.assertTrue(retry.run_once())
        with self.sessions() as session:
            candidates = list(session.scalars(select(ObjectGcCandidate)))
            self.assertEqual({candidate.state for candidate in candidates}, {"protected"})
            self.assertEqual(session.scalar(select(func.count()).select_from(ReportRevision)), 1)

    def test_attempt_deletion_suppresses_report_head_publication(self) -> None:
        self.service.admit(self.principal, self.segment())
        normalizer = OutboxWorker(self.sessions, self.objects, owner="normalizer-a")
        reports = ReportRevisionWorker(self.sessions, self.objects, owner="reporter-a")
        self.assertTrue(normalizer.run_once())
        lease = reports.claim()
        assert lease is not None

        self.service.delete_attempt("tenant-a", "cluster-a", "attempt-1")

        self.assertFalse(reports.execute(lease))
        with self.sessions() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(ReportRevision)), 0)
            self.assertEqual(session.scalar(select(func.count()).select_from(ReportHead)), 0)
            report_outbox = session.get(OutboxRecord, lease.outbox_id)
            assert report_outbox is not None
            self.assertEqual(report_outbox.state, "suppressed")

    def test_attempt_deletion_worker_erases_catalog_and_objects(self) -> None:
        receipt = self.service.admit(
            self.principal,
            self.segment(scheduler_source_identity="slurm:cluster-a:derived:delete-me"),
        )
        normalizer = OutboxWorker(self.sessions, self.objects, owner="normalizer-a")
        reporter = ReportRevisionWorker(self.sessions, self.objects, owner="reporter-a")
        self.assertTrue(normalizer.run_once())
        self.assertTrue(reporter.run_once())
        with self.sessions() as session:
            revision = session.scalar(select(ReportRevision))
            assert revision is not None
            report_key = revision.report_object_key
            report_digest = revision.report_sha256
            assert revision.parquet_object_key is not None
            assert revision.parquet_sha256 is not None
            parquet_key = revision.parquet_object_key
            parquet_digest = revision.parquet_sha256

        self.service.delete_attempt("tenant-a", "cluster-a", "attempt-1")
        deletions = AttemptDeletionWorker(
            self.sessions, self.objects, owner="deletion-a"
        )
        self.assertTrue(deletions.run_once())
        self.assertFalse(deletions.run_once())

        self.assertFalse(self.objects.exists_verified(receipt.object_key, receipt.payload_sha256))
        self.assertFalse(self.objects.exists_verified(report_key, report_digest))
        self.assertFalse(self.objects.exists_verified(parquet_key, parquet_digest))
        with self.sessions() as session:
            deletion = session.get(
                AttemptDeletion, ("tenant-a", "cluster-a", "attempt-1")
            )
            assert deletion is not None
            self.assertEqual(deletion.state, "completed")
            self.assertEqual(deletion.deleted_segment_objects, 1)
            self.assertEqual(deletion.deleted_report_objects, 2)
            for model in (
                AdmissionReservation,
                AttemptRecord,
                NormalizedSegment,
                OutboxRecord,
                ReportHead,
                ReportRevision,
                SegmentManifest,
            ):
                self.assertEqual(session.scalar(select(func.count()).select_from(model)), 0)

    def test_attempt_deletion_retains_a_segment_shared_by_another_attempt(self) -> None:
        first = self.service.admit(self.principal, self.segment())
        second_segment = self.segment(attempt_id="attempt-2").model_copy(
            update={"producer_id": "node-agent-2"}
        )
        self.service.admit(self.principal, second_segment)
        self.service.delete_attempt("tenant-a", "cluster-a", "attempt-1")

        worker = AttemptDeletionWorker(self.sessions, self.objects, owner="deletion-a")
        self.assertTrue(worker.run_once())

        self.assertTrue(self.objects.exists_verified(first.object_key, first.payload_sha256))
        with self.sessions() as session:
            deletion = session.get(
                AttemptDeletion, ("tenant-a", "cluster-a", "attempt-1")
            )
            assert deletion is not None
            self.assertEqual(deletion.retained_shared_objects, 1)
            remaining = list(session.scalars(select(SegmentManifest)))
            self.assertEqual([item.attempt_id for item in remaining], ["attempt-2"])

    def test_attempt_deletion_retries_object_provider_failure(self) -> None:
        receipt = self.service.admit(self.principal, self.segment())
        self.service.delete_attempt("tenant-a", "cluster-a", "attempt-1")

        class FailOnceStore:
            def __init__(self, delegate):
                self.delegate = delegate
                self.failed = False

            def delete_verified(self, object_key: str, sha256: str) -> bool:
                if not self.failed:
                    self.failed = True
                    raise ObjectIntegrityError("temporary provider failure")
                return self.delegate.delete_verified(object_key, sha256)

        worker = AttemptDeletionWorker(
            self.sessions, FailOnceStore(self.objects), owner="deletion-a"
        )
        self.assertFalse(worker.run_once())
        with self.sessions() as session:
            deletion = session.get(
                AttemptDeletion, ("tenant-a", "cluster-a", "attempt-1")
            )
            assert deletion is not None
            self.assertEqual(deletion.state, "pending")
            self.assertEqual(deletion.attempts, 1)
            self.assertIn("temporary provider failure", deletion.last_error or "")
        self.assertTrue(self.objects.exists_verified(receipt.object_key, receipt.payload_sha256))

        self.assertTrue(worker.run_once())
        self.assertFalse(self.objects.exists_verified(receipt.object_key, receipt.payload_sha256))

    def test_attempt_deletion_worker_rechecks_holds_after_claim(self) -> None:
        receipt = self.service.admit(self.principal, self.segment())
        self.service.delete_attempt("tenant-a", "cluster-a", "attempt-1")
        worker = AttemptDeletionWorker(self.sessions, self.objects, owner="deletion-a")
        lease = worker.claim()
        assert lease is not None
        hold_id = "00000000-0000-0000-0000-000000000063"
        now = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
        with self.sessions.begin() as session:
            session.add(
                AttemptHold(
                    hold_id=hold_id,
                    tenant_id="tenant-a",
                    cluster_id="cluster-a",
                    attempt_id="attempt-1",
                    reason="late compliance hold",
                    state="active",
                    placed_at=now,
                    released_at=None,
                )
            )

        self.assertFalse(worker.execute(lease))
        self.assertTrue(self.objects.exists_verified(receipt.object_key, receipt.payload_sha256))
        with self.sessions() as session:
            deletion = session.get(
                AttemptDeletion, ("tenant-a", "cluster-a", "attempt-1")
            )
            assert deletion is not None
            self.assertEqual(deletion.state, "held")
            self.assertIn("retention hold", deletion.last_error or "")

        self.assertTrue(self.service.release_attempt_hold(hold_id))
        self.assertTrue(worker.run_once())
        self.assertFalse(self.objects.exists_verified(receipt.object_key, receipt.payload_sha256))


if __name__ == "__main__":
    unittest.main()
