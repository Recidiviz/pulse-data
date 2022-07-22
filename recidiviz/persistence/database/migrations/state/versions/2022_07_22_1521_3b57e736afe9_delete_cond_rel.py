# pylint: skip-file
"""delete_cond_rel

Revision ID: 3b57e736afe9
Revises: cacf6a2cb22b
Create Date: 2022-07-22 15:21:50.105326

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3b57e736afe9"
down_revision = "cacf6a2cb22b"
branch_labels = None
depends_on = None


# With CONDITIONAL_RELEASE
old_values = [
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

# Without CONDITIONAL_RELEASE
new_values = [
    "ABSCONSION",
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
