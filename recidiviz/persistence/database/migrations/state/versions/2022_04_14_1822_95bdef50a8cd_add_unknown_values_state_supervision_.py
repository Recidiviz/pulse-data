# pylint: skip-file
"""add_unknown_values_state_supervision_violation_response_deciding_body_type

Revision ID: 95bdef50a8cd
Revises: 15fc233812ac
Create Date: 2022-04-14 18:22:17.328406

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "95bdef50a8cd"
down_revision = "15fc233812ac"
branch_labels = None
depends_on = None

# Without new value
old_values = ["COURT", "PAROLE_BOARD", "SUPERVISION_OFFICER"]

# With new value
new_values = [
    "COURT",
    "PAROLE_BOARD",
    "SUPERVISION_OFFICER",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_violation_response_deciding_body_type RENAME TO state_supervision_violation_response_deciding_body_type_old;"
    )
    sa.Enum(
        *new_values, name="state_supervision_violation_response_deciding_body_type"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_violation_response",
        column_name="deciding_body_type",
        type_=sa.Enum(
            *new_values, name="state_supervision_violation_response_deciding_body_type"
        ),
        postgresql_using="deciding_body_type::text::state_supervision_violation_response_deciding_body_type",
    )
    op.alter_column(
        "state_supervision_violation_response_history",
        column_name="deciding_body_type",
        type_=sa.Enum(
            *new_values, name="state_supervision_violation_response_deciding_body_type"
        ),
        postgresql_using="deciding_body_type::text::state_supervision_violation_response_deciding_body_type",
    )
    op.execute("DROP TYPE state_supervision_violation_response_deciding_body_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_violation_response_deciding_body_type RENAME TO state_supervision_violation_response_deciding_body_type_old;"
    )
    sa.Enum(
        *old_values, name="state_supervision_violation_response_deciding_body_type"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_violation_response",
        column_name="deciding_body_type",
        type_=sa.Enum(
            *old_values, name="state_supervision_violation_response_deciding_body_type"
        ),
        postgresql_using="deciding_body_type::text::state_supervision_violation_response_deciding_body_type",
    )
    op.alter_column(
        "state_supervision_violation_response_history",
        column_name="deciding_body_type",
        type_=sa.Enum(
            *old_values, name="state_supervision_violation_response_deciding_body_type"
        ),
        postgresql_using="deciding_body_type::text::state_supervision_violation_response_deciding_body_type",
    )
    op.execute("DROP TYPE state_supervision_violation_response_deciding_body_type_old;")
