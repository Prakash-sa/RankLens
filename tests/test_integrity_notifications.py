from __future__ import annotations

import hashlib
import hmac
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from ranklens_enterprise.database import (
    IntegrityNotification,
    build_engine,
    build_session_factory,
    initialize_schema,
)
from ranklens_enterprise.notifications import (
    IntegrityNotificationWorker,
    SignedWebhookSender,
)


class IntegrityNotificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        path = Path(self.temporary.name) / "catalog.sqlite3"
        self.engine = build_engine(f"sqlite:///{path}")
        initialize_schema(self.engine)
        self.sessions = build_session_factory(self.engine)
        self.now = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)

    def tearDown(self) -> None:
        self.engine.dispose()
        self.temporary.cleanup()

    def add_notification(self, notification_id: str) -> None:
        with self.sessions.begin() as session:
            session.add(IntegrityNotification(
                notification_id=notification_id,
                incident_id="00000000-0000-0000-0000-000000000101",
                tenant_id="tenant-a", cluster_id="cluster-a", occurrence=1,
                event_type="integrity.incident.detected",
                payload_json='{"event_type":"integrity.incident.detected"}',
                state="pending", attempts=0, next_attempt_at=self.now,
                lease_owner=None, lease_generation=0, lease_expires_at=None,
                last_error=None, created_at=self.now, delivered_at=None,
            ))

    def test_worker_retries_with_backoff_then_delivers_once_due(self) -> None:
        notification_id = "00000000-0000-0000-0000-000000000102"
        self.add_notification(notification_id)
        now = [self.now]
        deliveries: list[tuple[str, str, str]] = []

        def sender(*event: str) -> None:
            deliveries.append(event)
            if len(deliveries) == 1:
                raise RuntimeError("paging provider unavailable")

        worker = IntegrityNotificationWorker(
            self.sessions, sender, retry_base_seconds=30, clock=lambda: now[0]
        )
        self.assertTrue(worker.run_once())
        with self.sessions() as session:
            row = session.get(IntegrityNotification, notification_id)
            assert row is not None
            self.assertEqual(row.state, "pending")
            self.assertEqual(row.attempts, 1)
            self.assertIn("provider unavailable", row.last_error or "")
        self.assertFalse(worker.run_once())
        now[0] += timedelta(seconds=30)
        self.assertTrue(worker.run_once())
        with self.sessions() as session:
            row = session.get(IntegrityNotification, notification_id)
            assert row is not None
            self.assertEqual(row.state, "delivered")
            self.assertEqual(row.attempts, 2)
            self.assertIsNotNone(row.delivered_at)
            self.assertIsNone(row.last_error)
        self.assertEqual(len(deliveries), 2)
        self.assertEqual(deliveries[0], deliveries[1])

    def test_worker_quarantines_after_bounded_failures(self) -> None:
        notification_id = "00000000-0000-0000-0000-000000000103"
        self.add_notification(notification_id)
        now = [self.now]

        def fail(*_: str) -> None:
            raise RuntimeError("permanent rejection")

        worker = IntegrityNotificationWorker(
            self.sessions, fail, max_attempts=2,
            retry_base_seconds=1, clock=lambda: now[0],
        )
        self.assertTrue(worker.run_once())
        now[0] += timedelta(seconds=1)
        self.assertTrue(worker.run_once())
        now[0] += timedelta(days=1)
        self.assertFalse(worker.run_once())
        with self.sessions() as session:
            row = session.get(IntegrityNotification, notification_id)
            assert row is not None
            self.assertEqual(row.state, "failed")
            self.assertEqual(row.attempts, 2)
            self.assertIsNone(row.lease_owner)

    def test_sender_signs_exact_canonical_body_and_identity(self) -> None:
        captured: dict[str, object] = {}

        class Response:
            status = 204

            def __enter__(self):
                return self

            def __exit__(self, *_: object) -> None:
                return None

        def urlopen(request, *, timeout: float):
            captured["url"] = request.full_url
            captured["body"] = request.data
            captured["headers"] = {key.lower(): value for key, value in request.header_items()}
            captured["timeout"] = timeout
            return Response()

        payload = '{"event_type":"integrity.incident.detected"}'
        secret = "s" * 32
        sender = SignedWebhookSender(
            "https://alerts.example.test/ranklens", secret,
            timeout_seconds=7, clock=lambda: 1_790_000_000.9,
        )
        with patch("urllib.request.urlopen", side_effect=urlopen):
            sender("event-1", "integrity.incident.detected", payload)
        expected = hmac.new(
            secret.encode(), b"1790000000." + payload.encode(), hashlib.sha256
        ).hexdigest()
        headers = captured["headers"]
        assert isinstance(headers, dict)
        self.assertEqual(captured["body"], payload.encode())
        self.assertEqual(headers["x-ranklens-event-id"], "event-1")
        self.assertEqual(headers["x-ranklens-signature"], f"sha256={expected}")
        self.assertEqual(captured["timeout"], 7)


if __name__ == "__main__":
    unittest.main()
