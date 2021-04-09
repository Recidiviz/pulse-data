# pylint: skip-file
"""add_missing_vals_to_pfi

Revision ID: 8b4feda0caa1
Revises: 4b9155409eb1
Create Date: 2020-05-30 14:57:06.971805

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8b4feda0caa1"
down_revision = "4b9155409eb1"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "GENERAL",
    "PAROLE_BOARD_HOLD",
    "SHOCK_INCARCERATION",
    "TREATMENT_IN_PRISON",
]

# With new value
new_values = [
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "GENERAL",
    "PAROLE_BOARD_HOLD",
    "SHOCK_INCARCERATION",
    "TREATMENT_IN_PRISON",
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
