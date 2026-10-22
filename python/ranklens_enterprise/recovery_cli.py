"""Operator CLI for exporting and applying authenticated deletion tombstones."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

from .database import assert_schema_ready, build_engine, build_session_factory
from .recovery import apply_completed_tombstones, export_completed_tombstones


def _secret(path: Path) -> str:
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ValueError("tombstone secret file must be a regular non-symlink file")
    if metadata.st_mode & 0o077:
        raise ValueError("tombstone secret file must not be accessible by group or others")
    value = path.read_text(encoding="utf-8").strip()
    if not 32 <= len(value) <= 1024:
        raise ValueError("tombstone signing secret must contain 32 to 1024 characters")
    return value


def _write_new(path: Path, payload: bytes) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        try:
            path.unlink()
        except OSError:
            pass
        raise


def _cursor(value: str | None) -> tuple[str, str, str] | None:
    if value is None:
        return None
    parts = value.split(",")
    if len(parts) != 3 or any(not part for part in parts):
        raise ValueError("after cursor must be tenant_id,cluster_id,attempt_id")
    return parts[0], parts[1], parts[2]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Export or reconcile authenticated RankLens deletion tombstones"
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--secret-file", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    export = commands.add_parser("export")
    export.add_argument("--output", type=Path, required=True)
    export.add_argument("--limit", type=int, default=1000)
    export.add_argument("--after")
    apply = commands.add_parser("apply")
    apply.add_argument("--input", type=Path, required=True)
    apply.add_argument("--actor", required=True)
    args = parser.parse_args(argv)

    engine = build_engine(args.database_url)
    try:
        assert_schema_ready(engine)
        sessions = build_session_factory(engine)
        secret = _secret(args.secret_file)
        if args.command == "export":
            result = export_completed_tombstones(
                sessions, secret, created_at=datetime.now(timezone.utc),
                after=_cursor(args.after), limit=args.limit,
            )
            _write_new(args.output, result.bundle)
            print(json.dumps({
                "output": str(args.output),
                "sha256": hashlib.sha256(result.bundle).hexdigest(),
                "next_cursor": list(result.next_cursor) if result.next_cursor else None,
            }, sort_keys=True))
        else:
            result = apply_completed_tombstones(
                sessions, args.input.read_bytes(), secret, actor=args.actor,
            )
            print(json.dumps(result.__dict__, sort_keys=True))
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"recovery reconciliation failed: {exc}", file=sys.stderr)
        return 2
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
