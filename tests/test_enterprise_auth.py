from __future__ import annotations

import hashlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import HTTPException

from ranklens_enterprise.auth import MachineAuthenticator
from ranklens_enterprise.settings import MachineCredential, MachinePrincipal, Settings


class MachineAuthenticatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.token = "rotated-machine-token-with-enough-entropy"
        self.now = datetime(2026, 10, 4, 12, 0, tzinfo=timezone.utc)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def authenticator(
        self,
        *,
        not_before: datetime | None = None,
        expires_at: datetime | None = None,
    ) -> MachineAuthenticator:
        credential = MachineCredential(
            token_sha256=hashlib.sha256(self.token.encode("utf-8")).hexdigest(),
            principal=MachinePrincipal(
                "tenant-a",
                frozenset({"cluster-a"}),
                "agent-rotating",
                frozenset({"segments:write"}),
            ),
            not_before=not_before,
            expires_at=expires_at,
        )
        settings = Settings(
            database_url="sqlite:///:memory:",
            object_root=Path(self.temporary.name),
            machine_credentials=(credential,),
        )
        return MachineAuthenticator(settings, clock=lambda: self.now)

    def test_accepts_active_digest_and_rejects_unmatched_token(self) -> None:
        authenticator = self.authenticator(
            not_before=self.now - timedelta(minutes=1),
            expires_at=self.now + timedelta(minutes=1),
        )

        principal = authenticator.authenticate_ingest(f"Bearer {self.token}")

        self.assertEqual(principal.credential_id, "agent-rotating")
        with self.assertRaises(HTTPException) as rejected:
            authenticator.authenticate_ingest("Bearer wrong-token")
        self.assertEqual(rejected.exception.status_code, 401)

    def test_rejects_not_yet_valid_and_expired_credentials(self) -> None:
        for authenticator in (
            self.authenticator(not_before=self.now + timedelta(seconds=1)),
            self.authenticator(expires_at=self.now),
        ):
            with self.assertRaises(HTTPException) as rejected:
                authenticator.authenticate_ingest(f"Bearer {self.token}")
            self.assertEqual(rejected.exception.status_code, 401)
            self.assertEqual(rejected.exception.detail["code"], "machine_auth_invalid")


if __name__ == "__main__":
    unittest.main()
