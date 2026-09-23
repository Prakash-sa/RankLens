"""Make scheduler-to-attempt binding unambiguous within a cluster."""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "0009_attempt_scheduler_identity"
down_revision: Union[str, Sequence[str], None] = "0008_attempt_records"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("attempt_records") as batch:
        batch.create_unique_constraint(
            "uq_attempt_record_scheduler_identity",
            ["tenant_id", "cluster_id", "scheduler_source_identity"],
        )


def downgrade() -> None:
    with op.batch_alter_table("attempt_records") as batch:
        batch.drop_constraint(
            "uq_attempt_record_scheduler_identity", type_="unique"
        )
