# pylint: skip-file
"""add_refused_participation_status

Revision ID: 26f50952aaab
Revises: 6e69681e48da
Create Date: 2021-03-30 11:20:45.098506

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "26f50952aaab"
down_revision = "6e69681e48da"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "DENIED",
    "DISCHARGED",
    "EXTERNAL_UNKNOWN",
    "IN_PROGRESS",
    "PENDING",
    "PRESENT_WITHOUT_INFO",
]

# With new value
new_values = [
    "DENIED",
    "DISCHARGED",
    "EXTERNAL_UNKNOWN",
    "IN_PROGRESS",
    "PENDING",
    "PRESENT_WITHOUT_INFO",
    "REFUSED",
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
