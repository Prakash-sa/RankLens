"""Scoped machine authentication for the ingestion surface."""

from __future__ import annotations

import secrets

from fastapi import Header, HTTPException, status

from .settings import MachinePrincipal, Settings


class MachineAuthenticator:
    def __init__(self, settings: Settings):
        self._tokens = settings.machine_tokens

    def authenticate(self, authorization: str = Header(default="")) -> MachinePrincipal:
        scheme, separator, supplied = authorization.partition(" ")
        if separator != " " or scheme.lower() != "bearer" or not supplied:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"code": "machine_auth_required", "message": "machine authentication required"},
                headers={"WWW-Authenticate": "Bearer"},
            )
        for token, principal in self._tokens.items():
            if secrets.compare_digest(token, supplied):
                return principal
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "machine_auth_invalid", "message": "machine authentication failed"},
            headers={"WWW-Authenticate": "Bearer"},
        )
