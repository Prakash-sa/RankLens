"""Versioned contracts for authenticated enterprise segment admission."""

from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


Identifier = str


class SegmentUpload(BaseModel):
    """One immutable transport segment.

    Tenant scope is intentionally absent: the API derives it from the
    authenticated machine principal.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    envelope_major: Literal[2] = 2
    cluster_id: Identifier = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    attempt_id: Identifier = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    producer_id: Identifier = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    transport_epoch: Identifier = Field(
        min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$"
    )
    stream_id: Identifier = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    first_sequence: int = Field(ge=0, le=2**63 - 1)
    last_sequence: int = Field(ge=0, le=2**63 - 1)
    record_count: int = Field(ge=1, le=10_000_000)
    deletion_generation: int = Field(ge=0, le=2**63 - 1)
    compression: Literal["identity"] = "identity"
    content_type: Literal["application/x-ndjson"] = "application/x-ndjson"
    expanded_size_bytes: int = Field(ge=1, le=8 * 1024 * 1024)
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload_base64: str = Field(min_length=4, max_length=12 * 1024 * 1024)

    @model_validator(mode="after")
    def validate_range(self) -> "SegmentUpload":
        if self.last_sequence < self.first_sequence:
            raise ValueError("last_sequence must be greater than or equal to first_sequence")
        if self.record_count > self.last_sequence - self.first_sequence + 1:
            raise ValueError("record_count cannot exceed the declared sequence range")
        return self


class DurableReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    receipt_id: str
    status: Literal["DURABLE"] = "DURABLE"
    replayed: bool = False
    object_key: str
    payload_sha256: str
    committed_at: datetime


class ReceiptView(DurableReceipt):
    tenant_id: str
    cluster_id: str
    attempt_id: str
    producer_id: str
    transport_epoch: str
    stream_id: str
    first_sequence: int
    last_sequence: int


class SchedulerObservationView(BaseModel):
    model_config = ConfigDict(extra="forbid")

    observation_id: str
    tenant_id: str
    cluster_id: str
    adapter: str
    adapter_version: str
    source_identity: str
    job_id: str
    array_job_id: Optional[str] = None
    array_task_id: Optional[str] = None
    step_id: Optional[str] = None
    restart_count: int
    state: str
    state_reason: Optional[str] = None
    submitted_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    allocated_nodes: Optional[int] = None
    allocated_cpus: Optional[int] = None
    account: Optional[str] = None
    partition: Optional[str] = None
    user_ref: Optional[str] = None
    observed_at: datetime
    first_seen_at: datetime


class AllocationIntervalView(BaseModel):
    model_config = ConfigDict(extra="forbid")

    started_at: datetime
    ended_at: Optional[datetime] = None
    allocated_nodes: Optional[int] = None
    allocated_cpus: Optional[int] = None
    partition: Optional[str] = None


class AllocationTimelineView(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_identity: str
    job_id: str
    latest_state: str
    latest_observed_at: datetime
    coverage_status: Literal["observed", "unknown"]
    coverage_reasons: List[str]
    intervals: List[AllocationIntervalView]


class HealthStatus(BaseModel):
    status: Literal["ok"] = "ok"
    service: Literal["ranklens-enterprise-api"] = "ranklens-enterprise-api"
    envelope_major: Literal[2] = 2


class ErrorDetail(BaseModel):
    code: str
    message: str
    retriable: bool = False
    request_id: Optional[str] = None
