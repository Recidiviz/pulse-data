# pylint: skip-file
"""add_sup_vacated

Revision ID: e6ed1b4205e5
Revises: c44a454b254f
Create Date: 2022-07-12 10:47:44.599379

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e6ed1b4205e5"
down_revision = "c44a454b254f"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "ABSCONSION",
    "COMMUTED",
    "DEATH",
    "DISCHARGE",
    "DISMISSED",
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
]

# With new value
new_values = [
    "ABSCONSION",
    "COMMUTED",
    "DEATH",
    "DISCHARGE",
    "DISMISSED",
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
