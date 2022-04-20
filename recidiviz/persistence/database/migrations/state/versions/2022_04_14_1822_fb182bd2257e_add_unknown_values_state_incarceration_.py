# pylint: skip-file
"""add_unknown_values_state_incarceration_facility_security_level

Revision ID: fb182bd2257e
Revises: b531fdde7bb1
Create Date: 2022-04-14 18:22:14.082456

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "fb182bd2257e"
down_revision = "b531fdde7bb1"
branch_labels = None
depends_on = None

# Without new value
old_values = ["MAXIMUM", "MEDIUM", "MINIMUM"]

# With new value
new_values = [
    "MAXIMUM",
    "MEDIUM",
    "MINIMUM",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_facility_security_level RENAME TO state_incarceration_facility_security_level_old;"
    )
    sa.Enum(*new_values, name="state_incarceration_facility_security_level").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="facility_security_level",
        type_=sa.Enum(*new_values, name="state_incarceration_facility_security_level"),
        postgresql_using="facility_security_level::text::state_incarceration_facility_security_level",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="facility_security_level",
        type_=sa.Enum(*new_values, name="state_incarceration_facility_security_level"),
        postgresql_using="facility_security_level::text::state_incarceration_facility_security_level",
    )
    op.execute("DROP TYPE state_incarceration_facility_security_level_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_facility_security_level RENAME TO state_incarceration_facility_security_level_old;"
    )
    sa.Enum(*old_values, name="state_incarceration_facility_security_level").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="facility_security_level",
        type_=sa.Enum(*old_values, name="state_incarceration_facility_security_level"),
        postgresql_using="facility_security_level::text::state_incarceration_facility_security_level",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="facility_security_level",
        type_=sa.Enum(*old_values, name="state_incarceration_facility_security_level"),
        postgresql_using="facility_security_level::text::state_incarceration_facility_security_level",
    )
    op.execute("DROP TYPE state_incarceration_facility_security_level_old;")
