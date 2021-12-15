# pylint: skip-file
"""add_internal_unknown_incarceration_admission_reason

Revision ID: a0398559d189
Revises: a6fb0ed2091c
Create Date: 2020-02-19 18:15:56.393793

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a0398559d189"
down_revision = "a6fb0ed2091c"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "ADMITTED_IN_ERROR",
    "DUAL_REVOCATION",
    "EXTERNAL_UNKNOWN",
    "NEW_ADMISSION",
    "PAROLE_REVOCATION",
    "PROBATION_REVOCATION",
    "RETURN_FROM_ERRONEOUS_RELEASE",
    "RETURN_FROM_ESCAPE",
    "TEMPORARY_CUSTODY",
    "TRANSFER",
]


# With new value
new_values = [
    "ADMITTED_IN_ERROR",
    "DUAL_REVOCATION",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "NEW_ADMISSION",
    "PAROLE_REVOCATION",
    "PROBATION_REVOCATION",
    "RETURN_FROM_ERRONEOUS_RELEASE",
    "RETURN_FROM_ESCAPE",
    "TEMPORARY_CUSTODY",
    "TRANSFER",
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
