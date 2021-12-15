# pylint: skip-file
"""remove_specific_revocation

Revision ID: b6f1fc45c89c
Revises: a4212aff4eac
Create Date: 2021-11-30 11:54:24.051403

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b6f1fc45c89c"
down_revision = "a4212aff4eac"
branch_labels = None
depends_on = None

# With parole, probation, and dual revocation
old_values = [
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
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "RETURN_FROM_TEMPORARY_RELEASE",
    "TRANSFER_FROM_OTHER_JURISDICTION",
    "REVOCATION",
]

# Without parole, probation, and dual revocation
new_values = [
    "ADMITTED_IN_ERROR",
    "ADMITTED_FROM_SUPERVISION",
    "NEW_ADMISSION",
    "RETURN_FROM_ERRONEOUS_RELEASE",
    "RETURN_FROM_ESCAPE",
    "SANCTION_ADMISSION",
    "STATUS_CHANGE",
    "TEMPORARY_CUSTODY",
    "TRANSFER",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "RETURN_FROM_TEMPORARY_RELEASE",
    "TRANSFER_FROM_OTHER_JURISDICTION",
    "REVOCATION",
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
