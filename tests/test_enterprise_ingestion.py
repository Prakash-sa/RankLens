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
    AttemptRecord,
    NormalizedSegment,
    ObjectGcCandidate,
    OutboxRecord,
    ReportHead,
    ReportRevision,
    SegmentManifest,
    build_engine,
    build_session_factory,
    initialize_schema,
)
from ranklens_enterprise.ingestion import AdmissionConflict, AdmissionError, IngestionService
from ranklens_enterprise.settings import MachinePrincipal
from ranklens_enterprise.storage import LocalObjectStore
from ranklens_enterprise.storage import ObjectIntegrityError
from ranklens_enterprise.worker import (
    ObjectGcWorker,
    OutboxWorker,
    ReportRevisionWorker,
    ReservationSweeper,
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
        self.principal = MachinePrincipal("tenant-a", frozenset({"cluster-a"}))

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

        with self.assertRaises(AdmissionConflict) as raised:
            self.service.admit(self.principal, self.segment(first=1, last=1))

        self.assertEqual(generation, 1)
        self.assertEqual(raised.exception.code, "attempt_deleted")

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
        self.assertEqual(revisions[1].parquet_schema_version, "normalized-record-v1")
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


if __name__ == "__main__":
    unittest.main()
