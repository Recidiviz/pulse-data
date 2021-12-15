# pylint: skip-file
"""add_external_unknown_to_supervision_admission_reason

Revision ID: a1b99c7cc151
Revises: 62e8cba7e710
Create Date: 2019-11-07 12:57:20.640078

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b99c7cc151"
down_revision = "62e8cba7e710"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "CONDITIONAL_RELEASE",
    "COURT_SENTENCE",
    "RETURN_FROM_ABSCONSION",
    "RETURN_FROM_SUSPENSION",
]

# With new value
new_values = [
    "CONDITIONAL_RELEASE",
    "COURT_SENTENCE",
    "EXTERNAL_UNKNOWN",
    "RETURN_FROM_ABSCONSION",
    "RETURN_FROM_SUSPENSION",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_period_admission_reason RENAME TO state_supervision_period_admission_reason_old;"
    )
    sa.Enum(*new_values, name="state_supervision_period_admission_reason").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_period",
        column_name="admission_reason",
        type_=sa.Enum(*new_values, name="state_supervision_period_admission_reason"),
        postgresql_using="admission_reason::text::state_supervision_period_admission_reason",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="admission_reason",
        type_=sa.Enum(*new_values, name="state_supervision_period_admission_reason"),
        postgresql_using="admission_reason::text::state_supervision_period_admission_reason",
    )
    op.execute("DROP TYPE state_supervision_period_admission_reason_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_period_admission_reason RENAME TO state_supervision_period_admission_reason_old;"
    )
    sa.Enum(*old_values, name="state_supervision_period_admission_reason").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_period",
        column_name="admission_reason",
        type_=sa.Enum(*old_values, name="state_supervision_period_admission_reason"),
        postgresql_using="admission_reason::text::state_supervision_period_admission_reason",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="admission_reason",
        type_=sa.Enum(*old_values, name="state_supervision_period_admission_reason"),
        postgresql_using="admission_reason::text::state_supervision_period_admission_reason",
    )
    op.execute("DROP TYPE state_supervision_period_admission_reason_old;")
