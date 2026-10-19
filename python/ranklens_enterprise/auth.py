"""Scoped machine authentication for the ingestion surface."""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timezone
from typing import Callable

from fastapi import Header, HTTPException, status
from sqlalchemy.orm import Session, sessionmaker

from .database import MachineCredentialRevocation, lock_stream
from .settings import MachinePrincipal, Settings


class MachineAuthenticator:
    def __init__(
        self,
        settings: Settings,
        sessions: sessionmaker[Session],
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ):
        self._credentials = settings.machine_credentials
        self._sessions = sessions
        self._clock = clock
        self._credentials_by_id = {
            credential.principal.credential_id: credential
            for credential in self._credentials
        }
        if len(self._credentials_by_id) != len(self._credentials):
            raise RuntimeError("machine credential IDs must be unique")

    def _is_revoked(self, principal: MachinePrincipal) -> bool:
        with self._sessions() as session:
            return (
                session.get(
                    MachineCredentialRevocation,
                    (principal.tenant_id, principal.credential_id),
                )
                is not None
            )

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
            elif self._is_revoked(matched.principal):
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

    def authenticate_security_admin(
        self, authorization: str = Header(default="")
    ) -> MachinePrincipal:
        return self._authenticate(authorization, "credentials:revoke")

    def authenticate_integrity_admin(
        self, authorization: str = Header(default="")
    ) -> MachinePrincipal:
        return self._authenticate(authorization, "integrity:admin")

    def revoke_credential(
        self,
        principal: MachinePrincipal,
        credential_id: str,
        reason: str,
    ) -> MachineCredentialRevocation:
        reason = reason.strip()
        if not reason or len(reason) > 512:
            raise ValueError("revocation reason must contain between 1 and 512 characters")
        if credential_id == principal.credential_id:
            raise ValueError("a security credential cannot revoke itself")
        target = self._credentials_by_id.get(credential_id)
        if target is None or target.principal.tenant_id != principal.tenant_id:
            raise LookupError("machine credential does not exist")
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise RuntimeError("machine authentication clock must be timezone-aware")
        identity = (principal.tenant_id, credential_id)
        with self._sessions.begin() as session:
            lock_stream(
                session,
                f"machine-credential-revocation:{principal.tenant_id}:{credential_id}",
            )
            existing = session.get(MachineCredentialRevocation, identity)
            if existing is not None:
                if (
                    existing.reason != reason
                    or existing.revoked_by_credential_id != principal.credential_id
                ):
                    raise ValueError(
                        "machine credential already has a different revocation"
                    )
                return existing
            revocation = MachineCredentialRevocation(
                tenant_id=principal.tenant_id,
                credential_id=credential_id,
                reason=reason,
                revoked_by_credential_id=principal.credential_id,
                revoked_at=now,
            )
            session.add(revocation)
            return revocation
