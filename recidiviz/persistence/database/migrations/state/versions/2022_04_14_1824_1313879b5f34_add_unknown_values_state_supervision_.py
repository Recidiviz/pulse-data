# pylint: skip-file
"""add_unknown_values_state_supervision_violation_type

Revision ID: 1313879b5f34
Revises: 523623e0bdb3
Create Date: 2022-04-14 18:24:56.274349

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1313879b5f34"
down_revision = "523623e0bdb3"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "ABSCONDED",
    "ESCAPED",
    "FELONY",
    "LAW",
    "MISDEMEANOR",
    "MUNICIPAL",
    "TECHNICAL",
]

# With new value
new_values = [
    "ABSCONDED",
    "ESCAPED",
    "FELONY",
    "LAW",
    "MISDEMEANOR",
    "MUNICIPAL",
    "TECHNICAL",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_violation_type RENAME TO state_supervision_violation_type_old;"
    )
    sa.Enum(*new_values, name="state_supervision_violation_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_violation_type_entry",
        column_name="violation_type",
        type_=sa.Enum(*new_values, name="state_supervision_violation_type"),
        postgresql_using="violation_type::text::state_supervision_violation_type",
    )
    op.alter_column(
        "state_supervision_violation_type_entry_history",
        column_name="violation_type",
        type_=sa.Enum(*new_values, name="state_supervision_violation_type"),
        postgresql_using="violation_type::text::state_supervision_violation_type",
    )
    op.execute("DROP TYPE state_supervision_violation_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_violation_type RENAME TO state_supervision_violation_type_old;"
    )
    sa.Enum(*old_values, name="state_supervision_violation_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_violation_type_entry",
        column_name="violation_type",
        type_=sa.Enum(*old_values, name="state_supervision_violation_type"),
        postgresql_using="violation_type::text::state_supervision_violation_type",
    )
    op.alter_column(
        "state_supervision_violation_type_entry_history",
        column_name="violation_type",
        type_=sa.Enum(*old_values, name="state_supervision_violation_type"),
        postgresql_using="violation_type::text::state_supervision_violation_type",
    )
    op.execute("DROP TYPE state_supervision_violation_type_old;")
