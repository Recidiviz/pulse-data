# pylint: skip-file
"""add_unknown_values_state_program_assignment_discharge_reason

Revision ID: 6f676c302974
Revises: f101bd3b0545
Create Date: 2022-04-14 18:19:03.559072

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6f676c302974"
down_revision = "f101bd3b0545"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "EXTERNAL_UNKNOWN",
    "ABSCONDED",
    "ADVERSE_TERMINATION",
    "COMPLETED",
    "MOVED",
    "OPTED_OUT",
    "PROGRAM_TRANSFER",
    "REINCARCERATED",
]

# With new value
new_values = [
    "EXTERNAL_UNKNOWN",
    "ABSCONDED",
    "ADVERSE_TERMINATION",
    "COMPLETED",
    "MOVED",
    "OPTED_OUT",
    "PROGRAM_TRANSFER",
    "REINCARCERATED",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_program_assignment_discharge_reason RENAME TO state_program_assignment_discharge_reason_old;"
    )
    sa.Enum(*new_values, name="state_program_assignment_discharge_reason").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_program_assignment",
        column_name="discharge_reason",
        type_=sa.Enum(*new_values, name="state_program_assignment_discharge_reason"),
        postgresql_using="discharge_reason::text::state_program_assignment_discharge_reason",
    )
    op.alter_column(
        "state_program_assignment_history",
        column_name="discharge_reason",
        type_=sa.Enum(*new_values, name="state_program_assignment_discharge_reason"),
        postgresql_using="discharge_reason::text::state_program_assignment_discharge_reason",
    )
    op.execute("DROP TYPE state_program_assignment_discharge_reason_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_program_assignment_discharge_reason RENAME TO state_program_assignment_discharge_reason_old;"
    )
    sa.Enum(*old_values, name="state_program_assignment_discharge_reason").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_program_assignment",
        column_name="discharge_reason",
        type_=sa.Enum(*old_values, name="state_program_assignment_discharge_reason"),
        postgresql_using="discharge_reason::text::state_program_assignment_discharge_reason",
    )
    op.alter_column(
        "state_program_assignment_history",
        column_name="discharge_reason",
        type_=sa.Enum(*old_values, name="state_program_assignment_discharge_reason"),
        postgresql_using="discharge_reason::text::state_program_assignment_discharge_reason",
    )
    op.execute("DROP TYPE state_program_assignment_discharge_reason_old;")
