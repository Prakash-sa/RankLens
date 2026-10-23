from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from ranklens_enterprise.storage import (
    LocalObjectStore,
    ObjectIntegrityError,
    ObjectSizeLimitError,
)


class EnterpriseStorageTests(unittest.TestCase):
    def test_bounded_verified_read_enforces_limit_and_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LocalObjectStore(Path(directory))
            payload = b"immutable evidence"
            digest = hashlib.sha256(payload).hexdigest()
            store.put_verified("objects/evidence.bin", payload, digest)

            self.assertEqual(
                store.read_verified_bounded(
                    "objects/evidence.bin", digest, len(payload)
                ),
                payload,
            )
            with self.assertRaisesRegex(ObjectSizeLimitError, "read limit"):
                store.read_verified_bounded(
                    "objects/evidence.bin", digest, len(payload) - 1
                )
            with self.assertRaisesRegex(ObjectIntegrityError, "checksum"):
                store.read_verified_bounded(
                    "objects/evidence.bin", "0" * 64, len(payload)
                )

    def test_bounded_verified_read_rejects_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = b"outside object"
            target = root / "target.bin"
            target.write_bytes(payload)
            link = root / "linked.bin"
            link.symlink_to(target)
            store = LocalObjectStore(root)

            with self.assertRaisesRegex(ObjectIntegrityError, "cannot read"):
                store.read_verified_bounded(
                    "linked.bin", hashlib.sha256(payload).hexdigest(), len(payload)
                )

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
