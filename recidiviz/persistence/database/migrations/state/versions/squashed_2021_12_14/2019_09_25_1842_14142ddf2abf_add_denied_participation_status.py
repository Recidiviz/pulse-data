# pylint: skip-file
"""add_denied_participation_status

Revision ID: 14142ddf2abf
Revises: 60eef7097fbd
Create Date: 2019-09-25 18:42:00.812448

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "14142ddf2abf"
down_revision = "60eef7097fbd"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "EXTERNAL_UNKNOWN",
    "PRESENT_WITHOUT_INFO",
    "DISCHARGED",
    "IN_PROGRESS",
    "PENDING",
]

# With new value
new_values = [
    "EXTERNAL_UNKNOWN",
    "PRESENT_WITHOUT_INFO",
    "DISCHARGED",
    "IN_PROGRESS",
    "PENDING",
    "DENIED",
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
