"""Leased, signed delivery for integrity-incident notifications."""

from __future__ import annotations

import hashlib
import hmac
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from .database import (
    IntegrityNotification,
    IntegrityNotificationRedrive,
    lock_stream,
    utc_now,
)


@dataclass(frozen=True)
class NotificationLease:
    notification_id: str
    generation: int
    owner: str


class SignedWebhookSender:
    """POST canonical event JSON with an HMAC signature and bounded timeout."""

    def __init__(
        self,
        url: str,
        secret: str,
        *,
        key_id: str = "legacy",
        timeout_seconds: float = 10.0,
        clock: Callable[[], float] = time.time,
    ):
        self._url = url
        self._secret = secret.encode("utf-8")
        self._key_id = key_id
        self._timeout_seconds = timeout_seconds
        self._clock = clock

    def __call__(self, notification_id: str, event_type: str, payload_json: str) -> None:
        body = payload_json.encode("utf-8")
        timestamp = str(int(self._clock()))
        digest = hmac.new(
            self._secret, timestamp.encode("ascii") + b"." + body, hashlib.sha256
        ).hexdigest()
        request = urllib.request.Request(
            self._url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "User-Agent": "RankLens-integrity-webhook/1",
                "X-RankLens-Event-ID": notification_id,
                "X-RankLens-Event-Type": event_type,
                "X-RankLens-Key-ID": self._key_id,
                "X-RankLens-Timestamp": timestamp,
                "X-RankLens-Signature": f"sha256={digest}",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_seconds) as response:
                status = response.status
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"webhook returned HTTP {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise RuntimeError("webhook delivery failed") from exc
        if not 200 <= status < 300:
            raise RuntimeError(f"webhook returned HTTP {status}")


class IntegrityNotificationWorker:
    """Deliver outbox rows with fencing, exponential retry and terminal quarantine."""

    def __init__(
        self,
        sessions: sessionmaker[Session],
        sender: Callable[[str, str, str], None],
        *,
        owner: str | None = None,
        lease_seconds: int = 60,
        max_attempts: int = 8,
        retry_base_seconds: int = 30,
        clock=utc_now,
    ):
        if not 10 <= lease_seconds <= 3600:
            raise ValueError("lease_seconds must be within [10, 3600]")
        if not 1 <= max_attempts <= 100:
            raise ValueError("max_attempts must be within [1, 100]")
        if not 1 <= retry_base_seconds <= 3600:
            raise ValueError("retry_base_seconds must be within [1, 3600]")
        self._sessions = sessions
        self._sender = sender
        self._owner = owner or f"notification-worker-{uuid.uuid4()}"
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts
        self._retry_base_seconds = retry_base_seconds
        self._clock = clock

    def claim(self) -> NotificationLease | None:
        now = self._clock()
        with self._sessions.begin() as session:
            query = (
                select(IntegrityNotification)
                .where(
                    (
                        (IntegrityNotification.state == "pending")
                        & (IntegrityNotification.next_attempt_at <= now)
                    )
                    | (
                        (IntegrityNotification.state == "processing")
                        & (IntegrityNotification.lease_expires_at < now)
                    )
                )
                .order_by(
                    IntegrityNotification.next_attempt_at,
                    IntegrityNotification.notification_id,
                )
                .limit(1)
            )
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                query = query.with_for_update(skip_locked=True)
            notification = session.scalar(query)
            if notification is None:
                return None
            lock_stream(session, f"integrity-notification:{notification.notification_id}")
            notification.state = "processing"
            notification.attempts += 1
            notification.lease_owner = self._owner
            notification.lease_generation += 1
            notification.lease_expires_at = now + timedelta(seconds=self._lease_seconds)
            return NotificationLease(
                notification.notification_id,
                notification.lease_generation,
                self._owner,
            )

    def execute(self, lease: NotificationLease) -> bool:
        with self._sessions() as session:
            notification = session.get(IntegrityNotification, lease.notification_id)
            if (
                notification is None
                or notification.state != "processing"
                or notification.lease_owner != lease.owner
                or notification.lease_generation != lease.generation
            ):
                return False
            event_type = notification.event_type
            payload_json = notification.payload_json
        try:
            self._sender(lease.notification_id, event_type, payload_json)
        except Exception as exc:
            self._fail(lease, str(exc))
            return False
        now = self._clock()
        with self._sessions.begin() as session:
            notification = session.get(IntegrityNotification, lease.notification_id)
            if not self._owns(notification, lease):
                return False
            notification.state = "delivered"
            notification.lease_owner = None
            notification.lease_expires_at = None
            notification.last_error = None
            notification.delivered_at = now
            return True

    @staticmethod
    def _owns(notification: IntegrityNotification | None, lease: NotificationLease) -> bool:
        return bool(
            notification is not None
            and notification.state == "processing"
            and notification.lease_owner == lease.owner
            and notification.lease_generation == lease.generation
        )

    def _fail(self, lease: NotificationLease, message: str) -> None:
        now = self._clock()
        with self._sessions.begin() as session:
            notification = session.get(IntegrityNotification, lease.notification_id)
            if not self._owns(notification, lease):
                return
            notification.lease_owner = None
            notification.lease_expires_at = None
            notification.last_error = (message or "webhook delivery failed")[:512]
            if notification.attempts >= self._max_attempts:
                notification.state = "failed"
            else:
                notification.state = "pending"
                delay = min(
                    3600,
                    self._retry_base_seconds * (2 ** (notification.attempts - 1)),
                )
                notification.next_attempt_at = now + timedelta(seconds=delay)

    def run_once(self) -> bool:
        lease = self.claim()
        if lease is None:
            return False
        self.execute(lease)
        return True


class IntegrityNotificationService:
    """Operator-controlled, audited redrive of terminal delivery failures."""

    def __init__(self, sessions: sessionmaker[Session], *, clock=utc_now):
        self._sessions = sessions
        self._clock = clock

    def redrive(
        self,
        tenant_id: str,
        cluster_id: str,
        notification_id: str,
        *,
        actor: str,
        reason: str,
    ) -> IntegrityNotification:
        reason = reason.strip()
        if not reason or len(reason) > 512:
            raise ValueError("redrive reason must contain 1 to 512 characters")
        now = self._clock()
        with self._sessions.begin() as session:
            lock_stream(session, f"integrity-notification:{notification_id}")
            notification = session.get(IntegrityNotification, notification_id)
            if (
                notification is None
                or notification.tenant_id != tenant_id
                or notification.cluster_id != cluster_id
            ):
                raise LookupError("integrity notification not found")
            latest = session.scalar(
                select(IntegrityNotificationRedrive)
                .where(IntegrityNotificationRedrive.notification_id == notification_id)
                .order_by(
                    IntegrityNotificationRedrive.requested_at.desc(),
                    IntegrityNotificationRedrive.redrive_id.desc(),
                )
                .limit(1)
            )
            if notification.state != "failed":
                if (
                    notification.state in {"pending", "processing"}
                    and latest is not None
                    and latest.requested_by_credential_id == actor
                    and latest.reason == reason
                ):
                    return notification
                raise ValueError("only a failed integrity notification can be redriven")
            notification.state = "pending"
            notification.attempts = 0
            notification.next_attempt_at = now
            notification.lease_owner = None
            notification.lease_expires_at = None
            notification.lease_generation += 1
            notification.last_error = None
            notification.delivered_at = None
            session.add(IntegrityNotificationRedrive(
                redrive_id=str(uuid.uuid4()), notification_id=notification_id,
                tenant_id=tenant_id, cluster_id=cluster_id,
                requested_by_credential_id=actor, reason=reason,
                requested_at=now,
            ))
            return notification
