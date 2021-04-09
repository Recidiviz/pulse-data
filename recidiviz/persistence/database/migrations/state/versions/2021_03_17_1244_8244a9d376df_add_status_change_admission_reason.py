# pylint: skip-file
"""add_status_change_admission_reason

Revision ID: 8244a9d376df
Revises: c0817a3f8f69
Create Date: 2021-03-17 12:44:30.891072

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8244a9d376df"
down_revision = "c0817a3f8f69"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "ADMITTED_IN_ERROR",
    "NEW_ADMISSION",
    "PAROLE_REVOCATION",
    "PROBATION_REVOCATION",
    "DUAL_REVOCATION",
    "RETURN_FROM_ERRONEOUS_RELEASE",
    "RETURN_FROM_ESCAPE",
    "RETURN_FROM_SUPERVISION",
    "TEMPORARY_CUSTODY",
    "TRANSFER",
    "TRANSFERRED_FROM_OUT_OF_STATE",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]

# With new value
new_values = [
    "ADMITTED_IN_ERROR",
    "NEW_ADMISSION",
    "PAROLE_REVOCATION",
    "PROBATION_REVOCATION",
    "DUAL_REVOCATION",
    "RETURN_FROM_ERRONEOUS_RELEASE",
    "RETURN_FROM_ESCAPE",
    "RETURN_FROM_SUPERVISION",
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
