"""Immutable object storage interface and crash-aware local implementation."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path
from typing import Protocol


class ObjectIntegrityError(RuntimeError):
    pass


class ImmutableObjectStore(Protocol):
    def put_verified(self, object_key: str, payload: bytes, sha256: str) -> None: ...

    def exists_verified(self, object_key: str, sha256: str) -> bool: ...

    def read_verified(self, object_key: str, sha256: str) -> bytes: ...

    def delete_verified(self, object_key: str, sha256: str) -> bool: ...


class LocalObjectStore:
    """Filesystem adapter for development, tests, and disconnected pilots.

    It uses a same-directory fsync + replace protocol. It is not a substitute
    for validating a production S3/Ceph/Azure/GCS adapter's conditional writes.
    """

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o750)

    def _path(self, object_key: str) -> Path:
        candidate = (self.root / object_key).resolve()
        if self.root not in candidate.parents:
            raise ObjectIntegrityError("object key escapes configured root")
        return candidate

    @staticmethod
    def _digest(payload: bytes) -> str:
        return hashlib.sha256(payload).hexdigest()

    def exists_verified(self, object_key: str, sha256: str) -> bool:
        target = self._path(object_key)
        if not target.exists():
            return False
        try:
            actual = hashlib.sha256(target.read_bytes()).hexdigest()
        except OSError as exc:
            raise ObjectIntegrityError(f"cannot verify existing object: {exc}") from exc
        if actual != sha256:
            raise ObjectIntegrityError("existing object digest does not match its content key")
        return True

    def read_verified(self, object_key: str, sha256: str) -> bytes:
        target = self._path(object_key)
        try:
            payload = target.read_bytes()
        except OSError as exc:
            raise ObjectIntegrityError(f"cannot read object: {exc}") from exc
        if self._digest(payload) != sha256:
            raise ObjectIntegrityError("stored object failed checksum verification")
        return payload

    def put_verified(self, object_key: str, payload: bytes, sha256: str) -> None:
        if self._digest(payload) != sha256:
            raise ObjectIntegrityError("payload digest mismatch")
        target = self._path(object_key)
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
        if self.exists_verified(object_key, sha256):
            return

        descriptor, temporary_name = tempfile.mkstemp(prefix=".ranklens-", dir=target.parent)
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o640)
            with os.fdopen(descriptor, "wb", closefd=True) as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            if self._digest(temporary.read_bytes()) != sha256:
                raise ObjectIntegrityError("temporary object failed checksum verification")
            os.replace(temporary, target)
            directory_descriptor = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        finally:
            if temporary.exists():
                temporary.unlink()

    def delete_verified(self, object_key: str, sha256: str) -> bool:
        """Delete only the exact verified object and durably record the directory change."""

        target = self._path(object_key)
        if not target.exists():
            return False
        try:
            payload = target.read_bytes()
        except OSError as exc:
            raise ObjectIntegrityError(f"cannot verify object before deletion: {exc}") from exc
        if self._digest(payload) != sha256:
            raise ObjectIntegrityError("object scheduled for deletion failed checksum verification")
        try:
            target.unlink()
            directory_descriptor = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except OSError as exc:
            raise ObjectIntegrityError(f"cannot delete verified object: {exc}") from exc
        return True
