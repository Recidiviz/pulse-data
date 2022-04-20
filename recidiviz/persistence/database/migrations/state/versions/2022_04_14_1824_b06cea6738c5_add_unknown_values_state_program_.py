# pylint: skip-file
"""add_unknown_values_state_program_assignment_participation_status

Revision ID: b06cea6738c5
Revises: ad7b1b5b087d
Create Date: 2022-04-14 18:24:37.031075

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b06cea6738c5"
down_revision = "ad7b1b5b087d"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "EXTERNAL_UNKNOWN",
    "PRESENT_WITHOUT_INFO",
    "DENIED",
    "DISCHARGED",
    "IN_PROGRESS",
    "PENDING",
    "REFUSED",
]

# With new value
new_values = [
    "EXTERNAL_UNKNOWN",
    "PRESENT_WITHOUT_INFO",
    "DENIED",
    "DISCHARGED",
    "IN_PROGRESS",
    "PENDING",
    "REFUSED",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_program_assignment_participation_status RENAME TO state_program_assignment_participation_status_old;"
    )
    sa.Enum(*new_values, name="state_program_assignment_participation_status").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_program_assignment",
        column_name="participation_status",
        type_=sa.Enum(
            *new_values, name="state_program_assignment_participation_status"
        ),
        postgresql_using="participation_status::text::state_program_assignment_participation_status",
    )
    op.alter_column(
        "state_program_assignment_history",
        column_name="participation_status",
        type_=sa.Enum(
            *new_values, name="state_program_assignment_participation_status"
        ),
        postgresql_using="participation_status::text::state_program_assignment_participation_status",
    )
    op.execute("DROP TYPE state_program_assignment_participation_status_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_program_assignment_participation_status RENAME TO state_program_assignment_participation_status_old;"
    )
    sa.Enum(*old_values, name="state_program_assignment_participation_status").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_program_assignment",
        column_name="participation_status",
        type_=sa.Enum(
            *old_values, name="state_program_assignment_participation_status"
        ),
        postgresql_using="participation_status::text::state_program_assignment_participation_status",
    )
    op.alter_column(
        "state_program_assignment_history",
        column_name="participation_status",
        type_=sa.Enum(
            *old_values, name="state_program_assignment_participation_status"
        ),
        postgresql_using="participation_status::text::state_program_assignment_participation_status",
    )
    op.execute("DROP TYPE state_program_assignment_participation_status_old;")
