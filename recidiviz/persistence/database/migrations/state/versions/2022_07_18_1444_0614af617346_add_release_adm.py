# pylint: skip-file
"""add_release_adm

Revision ID: 0614af617346
Revises: bb17e1da6f3d
Create Date: 2022-07-18 14:44:17.689275

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0614af617346"
down_revision = "bb17e1da6f3d"
branch_labels = None
depends_on = None


# Without new value
old_values = [
    "ABSCONSION",
    "CONDITIONAL_RELEASE",
    "COURT_SENTENCE",
    "INVESTIGATION",
    "TRANSFER_FROM_OTHER_JURISDICTION",
    "TRANSFER_WITHIN_STATE",
    "RETURN_FROM_ABSCONSION",
    "RETURN_FROM_SUSPENSION",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]

# With new value
new_values = [
    "ABSCONSION",
    "CONDITIONAL_RELEASE",
    "RELEASE_FROM_INCARCERATION",
    "COURT_SENTENCE",
    "INVESTIGATION",
    "TRANSFER_FROM_OTHER_JURISDICTION",
    "TRANSFER_WITHIN_STATE",
    "RETURN_FROM_ABSCONSION",
    "RETURN_FROM_SUSPENSION",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
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
    op.execute("DROP TYPE state_supervision_period_admission_reason_old;")
