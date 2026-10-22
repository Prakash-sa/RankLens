from __future__ import annotations

import base64
import hashlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select, text

from ranklens_enterprise.contracts import SegmentUpload
from ranklens_enterprise.database import (
    AttemptDeletion,
    AttemptGeneration,
    AttemptHold,
    AttemptRecord,
    RestoreTombstoneImport,
    SegmentManifest,
    build_engine,
    build_session_factory,
    initialize_schema,
)
from ranklens_enterprise.ingestion import AdmissionError, IngestionService
from ranklens_enterprise.recovery import (
    apply_completed_tombstones,
    export_completed_tombstones,
)
from ranklens_enterprise.recovery_cli import main as recovery_main
from ranklens_enterprise.settings import MachinePrincipal
from ranklens_enterprise.storage import LocalObjectStore
from ranklens_enterprise.worker import AttemptDeletionWorker


class EnterpriseRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.root = root
        self.source_url = f"sqlite:///{root / 'source.sqlite3'}"
        self.restore_url = f"sqlite:///{root / 'restore.sqlite3'}"
        self.source_engine = build_engine(self.source_url)
        self.restore_engine = build_engine(self.restore_url)
        initialize_schema(self.source_engine)
        initialize_schema(self.restore_engine)
        for engine in (self.source_engine, self.restore_engine):
            with engine.begin() as connection:
                connection.execute(text(
                    "CREATE TABLE alembic_version (version_num VARCHAR(64) NOT NULL)"
                ))
        self.source_sessions = build_session_factory(self.source_engine)
        self.restore_sessions = build_session_factory(self.restore_engine)
        self.objects = LocalObjectStore(root / "objects")
        self.service = IngestionService(self.restore_sessions, self.objects)
        self.principal = MachinePrincipal(
            "tenant-a", frozenset({"cluster-a"}), "restore-test"
        )
        self.requested_at = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
        self.completed_at = self.requested_at + timedelta(minutes=1)
        self.secret = "restore-signing-secret-" + "x" * 32

    def tearDown(self) -> None:
        self.source_engine.dispose()
        self.restore_engine.dispose()
        self.temporary.cleanup()

    @staticmethod
    def segment() -> SegmentUpload:
        payload = b'{"record":"restored"}\n'
        return SegmentUpload(
            cluster_id="cluster-a", attempt_id="attempt-restore",
            producer_id="node-1", transport_epoch="epoch-1",
            stream_id="rank-summary", first_sequence=0, last_sequence=0,
            record_count=1, deletion_generation=0,
            expanded_size_bytes=len(payload),
            payload_sha256=hashlib.sha256(payload).hexdigest(),
            payload_base64=base64.b64encode(payload).decode(),
        )

    def completed_tombstone_bundle(self) -> bytes:
        with self.source_sessions.begin() as session:
            session.add(AttemptGeneration(
                tenant_id="tenant-a", cluster_id="cluster-a",
                attempt_id="attempt-restore", generation=1,
                deleted_at=self.completed_at,
            ))
            session.add(AttemptDeletion(
                tenant_id="tenant-a", cluster_id="cluster-a",
                attempt_id="attempt-restore", deletion_generation=1,
                state="completed", attempts=1, lease_owner=None,
                lease_generation=1, lease_expires_at=None,
                deleted_segment_objects=1, retained_shared_objects=0,
                deleted_report_objects=0, deleted_catalog_rows=3,
                last_error=None, requested_at=self.requested_at,
                completed_at=self.completed_at,
            ))
        exported = export_completed_tombstones(
            self.source_sessions, self.secret,
            created_at=self.completed_at + timedelta(minutes=1),
        )
        self.assertIsNone(exported.next_cursor)
        return exported.bundle

    def test_authenticated_tombstone_fences_and_erases_restored_data(self) -> None:
        receipt = self.service.admit(self.principal, self.segment())
        self.assertTrue(self.objects.exists_verified(receipt.object_key, receipt.payload_sha256))
        with self.restore_sessions.begin() as session:
            session.add(AttemptHold(
                hold_id="00000000-0000-0000-0000-000000000201",
                tenant_id="tenant-a", cluster_id="cluster-a",
                attempt_id="attempt-restore", reason="stale restored hold",
                placed_by_credential_id="retention-admin",
                released_by_credential_id=None, state="active",
                placed_at=self.requested_at - timedelta(days=1), released_at=None,
            ))
        bundle = self.completed_tombstone_bundle()

        result = apply_completed_tombstones(
            self.restore_sessions, bundle, self.secret,
            actor="restore-operator", clock=lambda: self.completed_at + timedelta(minutes=2),
        )
        self.assertEqual(result.applied_count, 1)
        self.assertFalse(result.replayed)
        replay = apply_completed_tombstones(
            self.restore_sessions, bundle, self.secret,
            actor="restore-operator", clock=lambda: self.completed_at + timedelta(minutes=3),
        )
        self.assertTrue(replay.replayed)
        with self.restore_sessions() as session:
            generation = session.get(
                AttemptGeneration, ("tenant-a", "cluster-a", "attempt-restore")
            )
            deletion = session.get(
                AttemptDeletion, ("tenant-a", "cluster-a", "attempt-restore")
            )
            assert generation is not None and deletion is not None
            self.assertEqual(generation.generation, 1)
            self.assertIsNotNone(generation.deleted_at)
            self.assertEqual(deletion.state, "pending")
            hold = session.get(
                AttemptHold, "00000000-0000-0000-0000-000000000201"
            )
            assert hold is not None
            self.assertEqual(hold.state, "released")
            self.assertEqual(hold.released_by_credential_id, "restore:restore-operator")
            self.assertEqual(
                len(list(session.scalars(select(RestoreTombstoneImport)))), 1
            )
        with self.assertRaises(AdmissionError) as rejected:
            self.service.admit(self.principal, self.segment())
        self.assertEqual(rejected.exception.code, "attempt_deleted")

        self.assertTrue(
            AttemptDeletionWorker(self.restore_sessions, self.objects).run_once()
        )
        with self.restore_sessions() as session:
            self.assertIsNone(session.scalar(select(AttemptRecord)))
            self.assertIsNone(session.scalar(select(SegmentManifest)))
            deletion = session.get(
                AttemptDeletion, ("tenant-a", "cluster-a", "attempt-restore")
            )
            assert deletion is not None
            self.assertEqual(deletion.state, "completed")
        self.assertFalse(self.objects.exists_verified(receipt.object_key, receipt.payload_sha256))

    def test_tampered_or_noncanonical_tombstone_bundle_fails_closed(self) -> None:
        bundle = self.completed_tombstone_bundle()
        tampered = bundle.replace(b"attempt-restore", b"attempt-revived")
        with self.assertRaisesRegex(ValueError, "signature"):
            apply_completed_tombstones(
                self.restore_sessions, tampered, self.secret,
                actor="restore-operator",
            )
        with self.assertRaisesRegex(ValueError, "signature"):
            apply_completed_tombstones(
                self.restore_sessions, bundle, "wrong-secret-" + "z" * 32,
                actor="restore-operator",
            )

    def test_operator_cli_uses_protected_secret_and_non_overwriting_bundle(self) -> None:
        self.completed_tombstone_bundle()
        self.service.admit(self.principal, self.segment())
        secret_path = self.root / "tombstone.key"
        secret_path.write_text(self.secret, encoding="utf-8")
        secret_path.chmod(0o600)
        bundle_path = self.root / "tombstones.json"
        self.assertEqual(recovery_main([
            "--database-url", self.source_url,
            "--secret-file", str(secret_path),
            "export", "--output", str(bundle_path),
        ]), 0)
        self.assertEqual(bundle_path.stat().st_mode & 0o777, 0o600)
        self.assertNotEqual(bundle_path.read_bytes(), b"")
        self.assertEqual(recovery_main([
            "--database-url", self.source_url,
            "--secret-file", str(secret_path),
            "export", "--output", str(bundle_path),
        ]), 2)
        self.assertEqual(recovery_main([
            "--database-url", self.restore_url,
            "--secret-file", str(secret_path),
            "apply", "--input", str(bundle_path), "--actor", "restore-operator",
        ]), 0)
        with self.restore_sessions() as session:
            generation = session.get(
                AttemptGeneration, ("tenant-a", "cluster-a", "attempt-restore")
            )
            assert generation is not None
            self.assertEqual(generation.generation, 1)
            self.assertIsNotNone(generation.deleted_at)

        insecure_secret = self.root / "insecure.key"
        insecure_secret.write_text(self.secret, encoding="utf-8")
        insecure_secret.chmod(0o644)
        self.assertEqual(recovery_main([
            "--database-url", self.restore_url,
            "--secret-file", str(insecure_secret),
            "apply", "--input", str(bundle_path), "--actor", "restore-operator",
        ]), 2)

    def test_newer_live_generation_blocks_restore_reconciliation(self) -> None:
        bundle = self.completed_tombstone_bundle()
        with self.restore_sessions.begin() as session:
            session.add(AttemptGeneration(
                tenant_id="tenant-a", cluster_id="cluster-a",
                attempt_id="attempt-restore", generation=2, deleted_at=None,
            ))
        with self.assertRaisesRegex(ValueError, "newer live"):
            apply_completed_tombstones(
                self.restore_sessions, bundle, self.secret,
                actor="restore-operator",
            )
        with self.restore_sessions() as session:
            self.assertEqual(
                len(list(session.scalars(select(RestoreTombstoneImport)))), 0
            )


if __name__ == "__main__":
    unittest.main()
