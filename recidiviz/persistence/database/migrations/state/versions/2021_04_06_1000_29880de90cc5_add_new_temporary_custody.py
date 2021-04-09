# pylint: skip-file
"""add_new_temporary_custody

Revision ID: 29880de90cc5
Revises: 60e6b436fe15
Create Date: 2021-04-06 10:00:10.124594

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "29880de90cc5"
down_revision = "60e6b436fe15"
branch_labels = None
depends_on = None

# Without `TEMPORARY_CUSTODY`
old_values = [
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "GENERAL",
    "PAROLE_BOARD_HOLD",
    "SHOCK_INCARCERATION",
    "TREATMENT_IN_PRISON",
]

# With `TEMPORARY_CUSTODY`
new_values = [
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "GENERAL",
    "PAROLE_BOARD_HOLD",
    "SHOCK_INCARCERATION",
    "TREATMENT_IN_PRISON",
    "TEMPORARY_CUSTODY",
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
