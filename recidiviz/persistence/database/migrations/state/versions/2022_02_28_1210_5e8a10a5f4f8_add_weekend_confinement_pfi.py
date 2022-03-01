# pylint: skip-file
"""add_weekend_confinement_pfi

Revision ID: 5e8a10a5f4f8
Revises: 517becd19d29
Create Date: 2022-02-28 12:10:17.988276

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5e8a10a5f4f8"
down_revision = "517becd19d29"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "GENERAL",
    "PAROLE_BOARD_HOLD",
    "SHOCK_INCARCERATION",
    "TREATMENT_IN_PRISON",
    "TEMPORARY_CUSTODY",
]

# With new value
new_values = [
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "GENERAL",
    "PAROLE_BOARD_HOLD",
    "SHOCK_INCARCERATION",
    "TREATMENT_IN_PRISON",
    "TEMPORARY_CUSTODY",
    "WEEKEND_CONFINEMENT",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_specialized_purpose_for_incarceration RENAME TO state_specialized_purpose_for_incarceration_old;"
    )
    sa.Enum(*new_values, name="state_specialized_purpose_for_incarceration").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="specialized_purpose_for_incarceration",
        type_=sa.Enum(*new_values, name="state_specialized_purpose_for_incarceration"),
        postgresql_using="specialized_purpose_for_incarceration::text::state_specialized_purpose_for_incarceration",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="specialized_purpose_for_incarceration",
        type_=sa.Enum(*new_values, name="state_specialized_purpose_for_incarceration"),
        postgresql_using="specialized_purpose_for_incarceration::text::state_specialized_purpose_for_incarceration",
    )
    op.execute("DROP TYPE state_specialized_purpose_for_incarceration_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_specialized_purpose_for_incarceration RENAME TO state_specialized_purpose_for_incarceration_old;"
    )
    sa.Enum(*old_values, name="state_specialized_purpose_for_incarceration").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="specialized_purpose_for_incarceration",
        type_=sa.Enum(*old_values, name="state_specialized_purpose_for_incarceration"),
        postgresql_using="specialized_purpose_for_incarceration::text::state_specialized_purpose_for_incarceration",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="specialized_purpose_for_incarceration",
        type_=sa.Enum(*old_values, name="state_specialized_purpose_for_incarceration"),
        postgresql_using="specialized_purpose_for_incarceration::text::state_specialized_purpose_for_incarceration",
    )
    op.execute("DROP TYPE state_specialized_purpose_for_incarceration_old;")
