# pylint: skip-file
"""add_unknown_values_state_incarceration_incident_type

Revision ID: 046e1b2aff8e
Revises: 8c858c1eb703
Create Date: 2022-04-14 18:18:31.530593

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "046e1b2aff8e"
down_revision = "8c858c1eb703"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "PRESENT_WITHOUT_INFO",
    "CONTRABAND",
    "DISORDERLY_CONDUCT",
    "ESCAPE",
    "MINOR_OFFENSE",
    "POSITIVE",
    "REPORT",
    "VIOLENCE",
]

# With new value
new_values = [
    "PRESENT_WITHOUT_INFO",
    "CONTRABAND",
    "DISORDERLY_CONDUCT",
    "ESCAPE",
    "MINOR_OFFENSE",
    "POSITIVE",
    "REPORT",
    "VIOLENCE",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_incident_type RENAME TO state_incarceration_incident_type_old;"
    )
    sa.Enum(*new_values, name="state_incarceration_incident_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_incident",
        column_name="incident_type",
        type_=sa.Enum(*new_values, name="state_incarceration_incident_type"),
        postgresql_using="incident_type::text::state_incarceration_incident_type",
    )
    op.alter_column(
        "state_incarceration_incident_history",
        column_name="incident_type",
        type_=sa.Enum(*new_values, name="state_incarceration_incident_type"),
        postgresql_using="incident_type::text::state_incarceration_incident_type",
    )
    op.execute("DROP TYPE state_incarceration_incident_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_incident_type RENAME TO state_incarceration_incident_type_old;"
    )
    sa.Enum(*old_values, name="state_incarceration_incident_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_incident",
        column_name="incident_type",
        type_=sa.Enum(*old_values, name="state_incarceration_incident_type"),
        postgresql_using="incident_type::text::state_incarceration_incident_type",
    )
    op.alter_column(
        "state_incarceration_incident_history",
        column_name="incident_type",
        type_=sa.Enum(*old_values, name="state_incarceration_incident_type"),
        postgresql_using="incident_type::text::state_incarceration_incident_type",
    )
    op.execute("DROP TYPE state_incarceration_incident_type_old;")
