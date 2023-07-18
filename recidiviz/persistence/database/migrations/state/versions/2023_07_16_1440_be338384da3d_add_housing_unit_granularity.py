# pylint: skip-file
"""add_housing_unit_granularity

Revision ID: be338384da3d
Revises: 674bbfc9bd0a
Create Date: 2023-07-16 14:40:32.333955

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "be338384da3d"
down_revision = "674bbfc9bd0a"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "PERMANENT_SOLITARY_CONFINEMENT",
    "TEMPORARY_SOLITARY_CONFINEMENT",
    "GENERAL",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]

# With new value
new_values = [
    "TEMPORARY_SOLITARY_CONFINEMENT",
    "DISCIPLINARY_SOLITARY_CONFINEMENT",
    "ADMINISTRATIVE_SOLITARY_CONFINEMENT",
    "PROTECTIVE_CUSTODY",
    "OTHER_SOLITARY_CONFINEMENT",
    "MENTAL_HEALTH_SOLITARY_CONFINEMENT",
    "HOSPITAL",
    "GENERAL",
    "PERMANENT_SOLITARY_CONFINEMENT",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_period_housing_unit_type RENAME TO state_incarceration_period_housing_unit_type_old;"
    )
    sa.Enum(*new_values, name="state_incarceration_period_housing_unit_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="housing_unit_type",
        type_=sa.Enum(*new_values, name="state_incarceration_period_housing_unit_type"),
        postgresql_using="housing_unit_type::text::state_incarceration_period_housing_unit_type",
    )
    op.execute("DROP TYPE state_incarceration_period_housing_unit_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_period_housing_unit_type RENAME TO state_incarceration_period_housing_unit_type_old;"
    )
    sa.Enum(*old_values, name="state_incarceration_period_housing_unit_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="housing_unit_type",
        type_=sa.Enum(*old_values, name="state_incarceration_period_housing_unit_type"),
        postgresql_using="housing_unit_type::text::state_incarceration_period_housing_unit_type",
    )
    op.execute("DROP TYPE state_incarceration_period_housing_unit_type_old;")
