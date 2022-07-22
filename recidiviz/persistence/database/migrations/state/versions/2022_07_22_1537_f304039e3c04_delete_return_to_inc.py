# pylint: skip-file
"""delete_return_to_inc

Revision ID: f304039e3c04
Revises: 3b57e736afe9
Create Date: 2022-07-22 15:37:59.958196

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f304039e3c04"
down_revision = "3b57e736afe9"
branch_labels = None
depends_on = None


# With RETURN_TO_INCARCERATION
old_values = [
    "ABSCONSION",
    "ADMITTED_TO_INCARCERATION",
    "COMMUTED",
    "DEATH",
    "DISCHARGE",
    "EXPIRATION",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "INVESTIGATION",
    "PARDONED",
    "TRANSFER_WITHIN_STATE",
    "TRANSFER_TO_OTHER_JURISDICTION",
    "RETURN_FROM_ABSCONSION",
    "RETURN_TO_INCARCERATION",
    "REVOCATION",
    "SUSPENSION",
    "VACATED",
]

# Without RETURN_TO_INCARCERATION
new_values = [
    "ABSCONSION",
    "ADMITTED_TO_INCARCERATION",
    "COMMUTED",
    "DEATH",
    "DISCHARGE",
    "EXPIRATION",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "INVESTIGATION",
    "PARDONED",
    "TRANSFER_WITHIN_STATE",
    "TRANSFER_TO_OTHER_JURISDICTION",
    "RETURN_FROM_ABSCONSION",
    "REVOCATION",
    "SUSPENSION",
    "VACATED",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_period_termination_reason RENAME TO state_supervision_period_termination_reason_old;"
    )
    sa.Enum(*new_values, name="state_supervision_period_termination_reason").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_period",
        column_name="termination_reason",
        type_=sa.Enum(*new_values, name="state_supervision_period_termination_reason"),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.execute("DROP TYPE state_supervision_period_termination_reason_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_period_termination_reason RENAME TO state_supervision_period_termination_reason_old;"
    )
    sa.Enum(*old_values, name="state_supervision_period_termination_reason").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_period",
        column_name="termination_reason",
        type_=sa.Enum(*old_values, name="state_supervision_period_termination_reason"),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.execute("DROP TYPE state_supervision_period_termination_reason_old;")
