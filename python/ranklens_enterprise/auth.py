"""Scoped machine authentication for the ingestion surface."""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timezone
from typing import Callable

from fastapi import Header, HTTPException, status

from .settings import MachinePrincipal, Settings


class MachineAuthenticator:
    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ):
        self._credentials = settings.machine_credentials
        self._clock = clock

    def _authenticate(self, authorization: str, permission: str) -> MachinePrincipal:
        scheme, separator, supplied = authorization.partition(" ")
        if separator != " " or scheme.lower() != "bearer" or not supplied:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"code": "machine_auth_required", "message": "machine authentication required"},
                headers={"WWW-Authenticate": "Bearer"},
            )
        supplied_sha256 = hashlib.sha256(supplied.encode("utf-8")).hexdigest()
        matched = None
        for credential in self._credentials:
            if secrets.compare_digest(credential.token_sha256, supplied_sha256):
                matched = credential
        if matched is not None:
            now = self._clock()
            if now.tzinfo is None or now.utcoffset() is None:
                raise RuntimeError("machine authentication clock must be timezone-aware")
            if (
                (matched.not_before is not None and now < matched.not_before)
                or (matched.expires_at is not None and now >= matched.expires_at)
            ):
                matched = None
            elif permission not in matched.principal.permissions:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail={
                        "code": "machine_auth_forbidden",
                        "message": "machine credential lacks the required permission",
                    },
                )
            else:
                return matched.principal
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "machine_auth_invalid", "message": "machine authentication failed"},
            headers={"WWW-Authenticate": "Bearer"},
        )

    def authenticate_ingest(self, authorization: str = Header(default="")) -> MachinePrincipal:
        return self._authenticate(authorization, "segments:write")

    def authenticate_read(self, authorization: str = Header(default="")) -> MachinePrincipal:
        return self._authenticate(authorization, "telemetry:read")

    def authenticate_retention_admin(
        self, authorization: str = Header(default="")
    ) -> MachinePrincipal:
        return self._authenticate(authorization, "retention:admin")
