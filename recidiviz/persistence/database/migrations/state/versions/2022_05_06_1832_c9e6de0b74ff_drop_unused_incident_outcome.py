# pylint: skip-file
"""drop-unused-incident-outcome

Revision ID: c9e6de0b74ff
Revises: ee0779ae4772
Create Date: 2022-05-06 17:02:28.545321

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c9e6de0b74ff"
down_revision = "ee0779ae4772"
branch_labels = None
depends_on = None

# With MISCELLANEOUS
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
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]

# Without MISCELLANEOUS
new_values = [
    "CELL_CONFINEMENT",
    "DISCIPLINARY_LABOR",
    "DISMISSED",
    "EXTERNAL_PROSECUTION",
    "FINANCIAL_PENALTY",
    "GOOD_TIME_LOSS",
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
    op.execute("DROP TYPE state_incarceration_incident_outcome_type_old;")
