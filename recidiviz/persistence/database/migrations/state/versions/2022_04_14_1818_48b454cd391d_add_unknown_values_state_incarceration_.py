# pylint: skip-file
"""add_unknown_values_state_incarceration_incident_outcome_type

Revision ID: 48b454cd391d
Revises: 046e1b2aff8e
Create Date: 2022-04-14 18:18:37.684094

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "48b454cd391d"
down_revision = "046e1b2aff8e"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "CELL_CONFINEMENT",
    "DISCIPLINARY_LABOR",
    "DISMISSED",
    "EXTERNAL_PROSECUTION",
    "FINANCIAL_PENALTY",
    "GOOD_TIME_LOSS",
    "MISCELLANEOUS",
    "NOT_GUILTY",
    "PRIVILEGE_LOSS",
    "RESTRICTED_CONFINEMENT",
    "SOLITARY",
    "TREATMENT",
    "WARNING",
]

# With new value
new_values = [
    "CELL_CONFINEMENT",
    "DISCIPLINARY_LABOR",
    "DISMISSED",
    "EXTERNAL_PROSECUTION",
    "FINANCIAL_PENALTY",
    "GOOD_TIME_LOSS",
    "MISCELLANEOUS",
    "NOT_GUILTY",
    "PRIVILEGE_LOSS",
    "RESTRICTED_CONFINEMENT",
    "SOLITARY",
    "TREATMENT",
    "WARNING",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_incident_outcome_type RENAME TO state_incarceration_incident_outcome_type_old;"
    )
    sa.Enum(*new_values, name="state_incarceration_incident_outcome_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_incident_outcome",
        column_name="outcome_type",
        type_=sa.Enum(*new_values, name="state_incarceration_incident_outcome_type"),
        postgresql_using="outcome_type::text::state_incarceration_incident_outcome_type",
    )
    op.alter_column(
        "state_incarceration_incident_outcome_history",
        column_name="outcome_type",
        type_=sa.Enum(*new_values, name="state_incarceration_incident_outcome_type"),
        postgresql_using="outcome_type::text::state_incarceration_incident_outcome_type",
    )
    op.execute("DROP TYPE state_incarceration_incident_outcome_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_incident_outcome_type RENAME TO state_incarceration_incident_outcome_type_old;"
    )
    sa.Enum(*old_values, name="state_incarceration_incident_outcome_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_incident_outcome",
        column_name="outcome_type",
        type_=sa.Enum(*old_values, name="state_incarceration_incident_outcome_type"),
        postgresql_using="outcome_type::text::state_incarceration_incident_outcome_type",
    )
    op.alter_column(
        "state_incarceration_incident_outcome_history",
        column_name="outcome_type",
        type_=sa.Enum(*old_values, name="state_incarceration_incident_outcome_type"),
        postgresql_using="outcome_type::text::state_incarceration_incident_outcome_type",
    )
    op.execute("DROP TYPE state_incarceration_incident_outcome_type_old;")
