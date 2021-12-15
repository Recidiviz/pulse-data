# pylint: skip-file
"""add_investigation_sp_termination_reason

Revision ID: de5e9f994f17
Revises: cc51102fcaba
Create Date: 2020-04-20 08:43:16.165670

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "de5e9f994f17"
down_revision = "cc51102fcaba"
branch_labels = None
depends_on = None

# Without new value
old_termination_values = [
    "EXTERNAL_UNKNOWN",
    "ABSCONSION",
    "DEATH",
    "DISCHARGE",
    "EXPIRATION",
    "RETURN_FROM_ABSCONSION",
    "REVOCATION",
    "SUSPENSION",
    "INTERNAL_UNKNOWN",
    "TRANSFER_OUT_OF_STATE",
    "TRANSFER_WITHIN_STATE",
]
old_admission_values = [
    "ABSCONSION",
    "CONDITIONAL_RELEASE",
    "COURT_SENTENCE",
    "EXTERNAL_UNKNOWN",
    "RETURN_FROM_ABSCONSION",
    "RETURN_FROM_SUSPENSION",
    "INTERNAL_UNKNOWN",
    "TRANSFER_OUT_OF_STATE",
    "TRANSFER_WITHIN_STATE",
]

# With new value
new_termination_values = [
    "EXTERNAL_UNKNOWN",
    "ABSCONSION",
    "DEATH",
    "DISCHARGE",
    "EXPIRATION",
    "INVESTIGATION",
    "RETURN_FROM_ABSCONSION",
    "REVOCATION",
    "SUSPENSION",
    "INTERNAL_UNKNOWN",
    "TRANSFER_OUT_OF_STATE",
    "TRANSFER_WITHIN_STATE",
]
new_admission_values = [
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


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_period_termination_reason RENAME TO state_supervision_period_termination_reason_old;"
    )
    sa.Enum(
        *new_termination_values, name="state_supervision_period_termination_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="termination_reason",
        type_=sa.Enum(
            *new_termination_values, name="state_supervision_period_termination_reason"
        ),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="termination_reason",
        type_=sa.Enum(
            *new_termination_values, name="state_supervision_period_termination_reason"
        ),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.execute("DROP TYPE state_supervision_period_termination_reason_old;")

    op.execute(
        "ALTER TYPE state_supervision_period_admission_reason RENAME TO state_supervision_period_admission_reason_old;"
    )
    sa.Enum(
        *new_admission_values, name="state_supervision_period_admission_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="admission_reason",
        type_=sa.Enum(
            *new_admission_values, name="state_supervision_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_supervision_period_admission_reason",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="admission_reason",
        type_=sa.Enum(
            *new_admission_values, name="state_supervision_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_supervision_period_admission_reason",
    )
    op.execute("DROP TYPE state_supervision_period_admission_reason_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_period_termination_reason RENAME TO state_supervision_period_termination_reason_old;"
    )
    sa.Enum(
        *old_termination_values, name="state_supervision_period_termination_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="termination_reason",
        type_=sa.Enum(
            *old_termination_values, name="state_supervision_period_termination_reason"
        ),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="termination_reason",
        type_=sa.Enum(
            *old_termination_values, name="state_supervision_period_termination_reason"
        ),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.execute("DROP TYPE state_supervision_period_termination_reason_old;")

    op.execute(
        "ALTER TYPE state_supervision_period_admission_reason RENAME TO state_supervision_period_admission_reason_old;"
    )
    sa.Enum(
        *old_admission_values, name="state_supervision_period_admission_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="admission_reason",
        type_=sa.Enum(
            *old_admission_values, name="state_supervision_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_supervision_period_admission_reason",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="admission_reason",
        type_=sa.Enum(
            *old_admission_values, name="state_supervision_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_supervision_period_admission_reason",
    )
    op.execute("DROP TYPE state_supervision_period_admission_reason_old;")
