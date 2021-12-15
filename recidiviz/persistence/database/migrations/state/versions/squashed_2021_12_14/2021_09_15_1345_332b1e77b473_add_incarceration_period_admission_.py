# pylint: skip-file
"""add_incarceration_period_admission_reason_return_from_temporary_release

Revision ID: 332b1e77b473
Revises: 8eb409388203
Create Date: 2021-09-15 13:45:45.336434

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "332b1e77b473"
down_revision = "8eb409388203"
branch_labels = None
depends_on = None

# Without new value
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
    "TRANSFERRED_FROM_OUT_OF_STATE",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]

# With new value
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
    "RETURN_FROM_TEMPORARY_RELEASE",
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
