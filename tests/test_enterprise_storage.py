from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from ranklens_enterprise.storage import LocalObjectStore, ObjectIntegrityError


class EnterpriseStorageTests(unittest.TestCase):
    def test_verified_delete_is_idempotent_and_rejects_digest_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LocalObjectStore(Path(directory))
            payload = b"immutable evidence"
            digest = hashlib.sha256(payload).hexdigest()
            store.put_verified("objects/evidence.bin", payload, digest)

            with self.assertRaisesRegex(ObjectIntegrityError, "checksum"):
                store.delete_verified("objects/evidence.bin", "0" * 64)
            self.assertTrue(store.exists_verified("objects/evidence.bin", digest))
            self.assertTrue(store.delete_verified("objects/evidence.bin", digest))
            self.assertFalse(store.delete_verified("objects/evidence.bin", digest))

    def test_verified_delete_rejects_keys_outside_object_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LocalObjectStore(Path(directory))
            with self.assertRaisesRegex(ObjectIntegrityError, "escapes"):
                store.delete_verified("../outside", "0" * 64)


if __name__ == "__main__":
    unittest.main()
