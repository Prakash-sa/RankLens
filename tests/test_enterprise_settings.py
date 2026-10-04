from __future__ import annotations

import json
import hashlib
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from ranklens_enterprise.settings import Settings


class EnterpriseSettingsTests(unittest.TestCase):
    @staticmethod
    def environment() -> dict[str, str]:
        token_sha256 = hashlib.sha256(
            b"test-token-with-at-least-24-characters"
        ).hexdigest()
        return {
            "RANKLENS_DATABASE_URL": "sqlite:///:memory:",
            "RANKLENS_OBJECT_ROOT": "/tmp/ranklens-test-objects",
            "RANKLENS_MACHINE_CREDENTIALS_JSON": json.dumps(
                {
                    "agent-primary": {
                        "token_sha256": token_sha256,
                        "tenant_id": "tenant-a",
                        "clusters": ["cluster-a"],
                        "permissions": ["segments:write", "telemetry:read"],
                        "not_before": "2026-10-01T00:00:00Z",
                        "expires_at": "2027-10-01T00:00:00+00:00",
                    }
                }
            ),
        }

    def test_parses_bounded_reservation_lifecycle_settings(self) -> None:
        environment = self.environment()
        environment.update(
            {
                "RANKLENS_RESERVATION_TTL_SECONDS": "1200",
                "RANKLENS_RESERVATION_SWEEP_SECONDS": "45",
                "RANKLENS_OBJECT_GC_GRACE_SECONDS": "7200",
                "RANKLENS_OBJECT_GC_SWEEP_SECONDS": "60",
                "RANKLENS_OBJECT_GC_AUDIT_INTERVAL_SECONDS": "43200",
                "RANKLENS_OBJECT_GC_AUDIT_SWEEP_SECONDS": "180",
                "RANKLENS_OBJECT_GC_AUDIT_BATCH_SIZE": "250",
                "RANKLENS_DEFAULT_RETENTION_SECONDS": "2592000",
                "RANKLENS_RETENTION_SWEEP_SECONDS": "120",
                "RANKLENS_RETENTION_SWEEP_BATCH_SIZE": "50",
            }
        )
        with patch.dict("os.environ", environment, clear=True):
            settings = Settings.from_environment()

        self.assertEqual(settings.reservation_ttl_seconds, 1200)
        self.assertEqual(settings.reservation_sweep_seconds, 45)
        self.assertEqual(settings.object_gc_grace_seconds, 7200)
        self.assertEqual(settings.object_gc_sweep_seconds, 60)
        self.assertEqual(settings.object_gc_audit_interval_seconds, 43200)
        self.assertEqual(settings.object_gc_audit_sweep_seconds, 180)
        self.assertEqual(settings.object_gc_audit_batch_size, 250)
        self.assertEqual(settings.default_retention_seconds, 2592000)
        self.assertEqual(settings.retention_sweep_seconds, 120)
        self.assertEqual(settings.retention_sweep_batch_size, 50)
        self.assertEqual(len(settings.machine_credentials), 1)
        credential = settings.machine_credentials[0]
        self.assertEqual(credential.principal.credential_id, "agent-primary")
        self.assertEqual(
            credential.principal.permissions,
            frozenset({"segments:write", "telemetry:read"}),
        )
        self.assertEqual(
            credential.not_before, datetime(2026, 10, 1, tzinfo=timezone.utc)
        )

    def test_rejects_unsafe_reservation_timing(self) -> None:
        environment = self.environment()
        environment["RANKLENS_RESERVATION_TTL_SECONDS"] = "10"
        with patch.dict("os.environ", environment, clear=True):
            with self.assertRaisesRegex(RuntimeError, "TTL_SECONDS"):
                Settings.from_environment()

    def test_rejects_unsafe_object_gc_audit_policy(self) -> None:
        environment = self.environment()
        environment["RANKLENS_OBJECT_GC_AUDIT_BATCH_SIZE"] = "0"
        with patch.dict("os.environ", environment, clear=True):
            with self.assertRaisesRegex(RuntimeError, "AUDIT_BATCH_SIZE"):
                Settings.from_environment()

    def test_rejects_unsafe_default_retention(self) -> None:
        environment = self.environment()
        environment["RANKLENS_DEFAULT_RETENTION_SECONDS"] = "300"
        with patch.dict("os.environ", environment, clear=True):
            with self.assertRaisesRegex(RuntimeError, "DEFAULT_RETENTION_SECONDS"):
                Settings.from_environment()

    def test_rejects_plaintext_or_overprivileged_machine_credentials(self) -> None:
        environment = self.environment()
        credentials = json.loads(environment["RANKLENS_MACHINE_CREDENTIALS_JSON"])
        credentials["agent-primary"].pop("token_sha256")
        credentials["agent-primary"]["token"] = "plaintext-must-not-be-configured"
        environment["RANKLENS_MACHINE_CREDENTIALS_JSON"] = json.dumps(credentials)
        with patch.dict("os.environ", environment, clear=True):
            with self.assertRaisesRegex(RuntimeError, "unknown fields"):
                Settings.from_environment()

        environment = self.environment()
        credentials = json.loads(environment["RANKLENS_MACHINE_CREDENTIALS_JSON"])
        credentials["agent-primary"]["permissions"] = ["segments:write", "admin:all"]
        environment["RANKLENS_MACHINE_CREDENTIALS_JSON"] = json.dumps(credentials)
        with patch.dict("os.environ", environment, clear=True):
            with self.assertRaisesRegex(RuntimeError, "permissions"):
                Settings.from_environment()

    def test_rejects_invalid_machine_credential_validity_window(self) -> None:
        environment = self.environment()
        credentials = json.loads(environment["RANKLENS_MACHINE_CREDENTIALS_JSON"])
        credentials["agent-primary"]["expires_at"] = "2026-09-30T00:00:00Z"
        environment["RANKLENS_MACHINE_CREDENTIALS_JSON"] = json.dumps(credentials)
        with patch.dict("os.environ", environment, clear=True):
            with self.assertRaisesRegex(RuntimeError, "after not_before"):
                Settings.from_environment()


if __name__ == "__main__":
    unittest.main()
