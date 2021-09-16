# pylint: skip-file
"""remove_sp_admission_tranfer_out_of_state

Revision ID: 2ffde471f7cf
Revises: bc74f075e417
Create Date: 2021-09-16 12:08:01.326643

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2ffde471f7cf"
down_revision = "bc74f075e417"
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
    "TRANSFER_FROM_OTHER_JURISDICTION",
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
