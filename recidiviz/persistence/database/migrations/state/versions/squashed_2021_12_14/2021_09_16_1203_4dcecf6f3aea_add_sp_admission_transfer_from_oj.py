# pylint: skip-file
"""add_sp_admission_transfer_from_oj

Revision ID: 4dcecf6f3aea
Revises: 20fc4eb44eeb
Create Date: 2021-09-16 12:02:42.696147

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "4dcecf6f3aea"
down_revision = "20fc4eb44eeb"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "ABSCONSION",
    "CONDITIONAL_RELEASE",
    "COURT_SENTENCE",
    "EXTERNAL_UNKNOWN",
    "INVESTIGATION",
    "RETURN_FROM_ABSCONSION",
    "RETURN_FROM_SUSPENSION",
    "INTERNAL_UNKNOWN",
    "TRANSFER_OUT_OF_STATE",
    "TRANSFER_WITHIN_STATE",
]

# With new value
new_values = [
    "ABSCONSION",
    "CONDITIONAL_RELEASE",
    "COURT_SENTENCE",
    "EXTERNAL_UNKNOWN",
    "INVESTIGATION",
    "RETURN_FROM_ABSCONSION",
    "RETURN_FROM_SUSPENSION",
    "INTERNAL_UNKNOWN",
    "TRANSFER_OUT_OF_STATE",
    "TRANSFER_WITHIN_STATE",
    "TRANSFER_FROM_OTHER_JURISDICTION",
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
