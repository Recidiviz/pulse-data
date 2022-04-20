# pylint: skip-file
"""add_unknown_values_state_supervision_violation_response_type

Revision ID: 6fa63084ef17
Revises: 68f7c5087963
Create Date: 2022-04-14 18:18:56.827245

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6fa63084ef17"
down_revision = "68f7c5087963"
branch_labels = None
depends_on = None

# Without new value
old_values = ["CITATION", "VIOLATION_REPORT", "PERMANENT_DECISION"]

# With new value
new_values = [
    "CITATION",
    "VIOLATION_REPORT",
    "PERMANENT_DECISION",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_violation_response_type RENAME TO state_supervision_violation_response_type_old;"
    )
    sa.Enum(*new_values, name="state_supervision_violation_response_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_violation_response",
        column_name="response_type",
        type_=sa.Enum(*new_values, name="state_supervision_violation_response_type"),
        postgresql_using="response_type::text::state_supervision_violation_response_type",
    )
    op.alter_column(
        "state_supervision_violation_response_history",
        column_name="response_type",
        type_=sa.Enum(*new_values, name="state_supervision_violation_response_type"),
        postgresql_using="response_type::text::state_supervision_violation_response_type",
    )
    op.execute("DROP TYPE state_supervision_violation_response_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_violation_response_type RENAME TO state_supervision_violation_response_type_old;"
    )
    sa.Enum(*old_values, name="state_supervision_violation_response_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_violation_response",
        column_name="response_type",
        type_=sa.Enum(*old_values, name="state_supervision_violation_response_type"),
        postgresql_using="response_type::text::state_supervision_violation_response_type",
    )
    op.alter_column(
        "state_supervision_violation_response_history",
        column_name="response_type",
        type_=sa.Enum(*old_values, name="state_supervision_violation_response_type"),
        postgresql_using="response_type::text::state_supervision_violation_response_type",
    )
    op.execute("DROP TYPE state_supervision_violation_response_type_old;")
