"""Persist scheduler observations for attempt reconciliation."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0002_scheduler_observations"
down_revision: Union[str, Sequence[str], None] = "0001_enterprise_admission"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "scheduler_observations",
        sa.Column("observation_id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("cluster_id", sa.String(length=128), nullable=False),
        sa.Column("adapter", sa.String(length=64), nullable=False),
        sa.Column("adapter_version", sa.String(length=64), nullable=False),
        sa.Column("source_identity", sa.String(length=256), nullable=False),
        sa.Column("job_id", sa.String(length=128), nullable=False),
        sa.Column("array_job_id", sa.String(length=128), nullable=True),
        sa.Column("array_task_id", sa.String(length=128), nullable=True),
        sa.Column("step_id", sa.String(length=128), nullable=True),
        sa.Column("restart_count", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("state_reason", sa.String(length=256), nullable=True),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("allocated_nodes", sa.Integer(), nullable=True),
        sa.Column("allocated_cpus", sa.Integer(), nullable=True),
        sa.Column("account", sa.String(length=128), nullable=True),
        sa.Column("partition", sa.String(length=128), nullable=True),
        sa.Column("user_ref", sa.String(length=128), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_json", sa.Text(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("observation_id"),
        sa.UniqueConstraint(
            "tenant_id", "cluster_id", "source_identity", "observed_at",
            name="uq_scheduler_observation_seen",
        ),
    )
    op.create_index(
        "ix_scheduler_observation_attempt",
        "scheduler_observations",
        ["tenant_id", "cluster_id", "source_identity", "observed_at"],
    )
    op.create_index(
        "ix_scheduler_observation_job",
        "scheduler_observations",
        ["tenant_id", "cluster_id", "job_id", "observed_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_scheduler_observation_job", table_name="scheduler_observations")
    op.drop_index("ix_scheduler_observation_attempt", table_name="scheduler_observations")
    op.drop_table("scheduler_observations")
