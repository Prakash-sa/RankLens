from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from ranklens_enterprise.settings import Settings


class EnterpriseSettingsTests(unittest.TestCase):
    @staticmethod
    def environment() -> dict[str, str]:
        return {
            "RANKLENS_DATABASE_URL": "sqlite:///:memory:",
            "RANKLENS_OBJECT_ROOT": "/tmp/ranklens-test-objects",
            "RANKLENS_MACHINE_TOKENS_JSON": json.dumps(
                {
                    "test-token-with-at-least-24-characters": {
                        "tenant_id": "tenant-a",
                        "clusters": ["cluster-a"],
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
            }
        )
        with patch.dict("os.environ", environment, clear=True):
            settings = Settings.from_environment()

        self.assertEqual(settings.reservation_ttl_seconds, 1200)
        self.assertEqual(settings.reservation_sweep_seconds, 45)

    def test_rejects_unsafe_reservation_timing(self) -> None:
        environment = self.environment()
        environment["RANKLENS_RESERVATION_TTL_SECONDS"] = "10"
        with patch.dict("os.environ", environment, clear=True):
            with self.assertRaisesRegex(RuntimeError, "TTL_SECONDS"):
                Settings.from_environment()


if __name__ == "__main__":
    unittest.main()
