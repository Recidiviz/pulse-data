# pylint: skip-file
"""remove_return_from_supervision

Revision ID: b37d19a57acc
Revises: 2f89a19901c3
Create Date: 2021-04-15 12:57:14.927213

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b37d19a57acc"
down_revision = "2f89a19901c3"
branch_labels = None
depends_on = None

# With RETURN_FROM_SUPERVISION
old_values = [
    "ADMITTED_IN_ERROR",
    "ADMITTED_FROM_SUPERVISION",
    "NEW_ADMISSION",
    "PAROLE_REVOCATION",
    "PROBATION_REVOCATION",
    "DUAL_REVOCATION",
    "RETURN_FROM_ERRONEOUS_RELEASE",
    "RETURN_FROM_ESCAPE",
    "RETURN_FROM_SUPERVISION",
    "SANCTION_ADMISSION",
    "STATUS_CHANGE",
    "TEMPORARY_CUSTODY",
    "TRANSFER",
    "TRANSFERRED_FROM_OUT_OF_STATE",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]

# Without RETURN_FROM_SUPERVISION
new_values = [
    "ADMITTED_IN_ERROR",
    "ADMITTED_FROM_SUPERVISION",
    "NEW_ADMISSION",
    "PAROLE_REVOCATION",
    "PROBATION_REVOCATION",
    "DUAL_REVOCATION",
    "RETURN_FROM_ERRONEOUS_RELEASE",
    "RETURN_FROM_ESCAPE",
    "SANCTION_ADMISSION",
    "STATUS_CHANGE",
    "TEMPORARY_CUSTODY",
    "TRANSFER",
    "TRANSFERRED_FROM_OUT_OF_STATE",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_period_admission_reason RENAME TO state_incarceration_period_admission_reason_old;"
    )
    sa.Enum(*new_values, name="state_incarceration_period_admission_reason").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="admission_reason",
        type_=sa.Enum(*new_values, name="state_incarceration_period_admission_reason"),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="admission_reason",
        type_=sa.Enum(*new_values, name="state_incarceration_period_admission_reason"),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_admission_reason_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_period_admission_reason RENAME TO state_incarceration_period_admission_reason_old;"
    )
    sa.Enum(*old_values, name="state_incarceration_period_admission_reason").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="admission_reason",
        type_=sa.Enum(*old_values, name="state_incarceration_period_admission_reason"),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="admission_reason",
        type_=sa.Enum(*old_values, name="state_incarceration_period_admission_reason"),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_admission_reason_old;")
