"""Immutable object storage interface and crash-aware local implementation."""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from pathlib import Path
from typing import Protocol


class ObjectIntegrityError(RuntimeError):
    pass


class ObjectSizeLimitError(ObjectIntegrityError):
    """Raised before returning an object that exceeds a caller's memory budget."""


class ImmutableObjectStore(Protocol):
    def put_verified(self, object_key: str, payload: bytes, sha256: str) -> None: ...

    def exists_verified(self, object_key: str, sha256: str) -> bool: ...

    def read_verified(self, object_key: str, sha256: str) -> bytes: ...

    def read_verified_bounded(
        self, object_key: str, sha256: str, max_bytes: int
    ) -> bytes: ...

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

    def read_verified_bounded(
        self, object_key: str, sha256: str, max_bytes: int
    ) -> bytes:
        """Read and verify one regular file without exceeding ``max_bytes``.

        The descriptor is opened without following a final-component symlink, then
        checked again with ``fstat`` so an object replacement cannot turn a bounded
        evidence download into an unbounded or special-file read.
        """

        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        target = self.root / object_key
        resolved = target.resolve()
        if self.root not in resolved.parents:
            raise ObjectIntegrityError("object key escapes configured root")
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(target, flags)
        except OSError as exc:
            raise ObjectIntegrityError(f"cannot read object: {exc}") from exc
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise ObjectIntegrityError("stored object is not a regular file")
            if metadata.st_size > max_bytes:
                raise ObjectSizeLimitError("stored object exceeds the read limit")
            chunks = []
            remaining = max_bytes + 1
            while remaining:
                chunk = os.read(descriptor, min(1024 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
        except OSError as exc:
            raise ObjectIntegrityError(f"cannot read object: {exc}") from exc
        finally:
            os.close(descriptor)
        if len(payload) > max_bytes:
            raise ObjectSizeLimitError("stored object exceeds the read limit")
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
