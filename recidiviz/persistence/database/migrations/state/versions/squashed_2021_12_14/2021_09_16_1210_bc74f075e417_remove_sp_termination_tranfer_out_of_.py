# pylint: skip-file
"""remove_sp_termination_tranfer_out_of_state

Revision ID: bc74f075e417
Revises: b6601f72d22f
Create Date: 2021-09-16 12:07:29.033543

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "bc74f075e417"
down_revision = "b6601f72d22f"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "EXTERNAL_UNKNOWN",
    "ABSCONSION",
    "COMMUTED",
    "DEATH",
    "DISCHARGE",
    "DISMISSED",
    "EXPIRATION",
    "INVESTIGATION",
    "PARDONED",
    "RETURN_FROM_ABSCONSION",
    "RETURN_TO_INCARCERATION",
    "REVOCATION",
    "SUSPENSION",
    "INTERNAL_UNKNOWN",
    "TRANSFER_OUT_OF_STATE",
    "TRANSFER_WITHIN_STATE",
    "TRANSFER_TO_OTHER_JURISDICTION",
]

# With new value
new_values = [
    "EXTERNAL_UNKNOWN",
    "ABSCONSION",
    "COMMUTED",
    "DEATH",
    "DISCHARGE",
    "DISMISSED",
    "EXPIRATION",
    "INVESTIGATION",
    "PARDONED",
    "RETURN_FROM_ABSCONSION",
    "RETURN_TO_INCARCERATION",
    "REVOCATION",
    "SUSPENSION",
    "INTERNAL_UNKNOWN",
    "TRANSFER_WITHIN_STATE",
    "TRANSFER_TO_OTHER_JURISDICTION",
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
    op.alter_column(
        "state_supervision_period_history",
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
    op.alter_column(
        "state_supervision_period_history",
        column_name="termination_reason",
        type_=sa.Enum(*old_values, name="state_supervision_period_termination_reason"),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.execute("DROP TYPE state_supervision_period_termination_reason_old;")
