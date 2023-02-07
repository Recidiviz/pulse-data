# pylint: skip-file
"""pa_incident_metadata

Revision ID: cc34e32d49e0
Revises: 72e38c6f58a8
Create Date: 2023-02-06 13:06:25.857683

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "cc34e32d49e0"
down_revision = "72e38c6f58a8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "UPDATE state_incarceration_incident "
        "SET incident_metadata = incident_details, incident_details = NULL "
        "WHERE state_code = 'US_PA';"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE state_incarceration_incident "
        "SET incident_details = incident_metadata, incident_metadata = NULL "
        "WHERE state_code = 'US_PA';"
    )
