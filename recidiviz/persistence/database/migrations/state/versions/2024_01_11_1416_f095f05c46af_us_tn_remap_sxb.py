# pylint: skip-file
"""us_tn_remap_sxb

Revision ID: f095f05c46af
Revises: 97d31c236146
Create Date: 2024-01-11 14:16:05.492053

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f095f05c46af"
down_revision = "97d31c236146"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_incident
        SET incident_type = 'VIOLENCE'
        WHERE
            state_code = 'US_TN'
            AND incident_type_raw_text = 'SXB';
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_incident
        SET incident_type = 'INTERNAL_UNKNOWN'
        WHERE
            state_code = 'US_TN'
            AND incident_type_raw_text = 'SXB';
        """
    )
