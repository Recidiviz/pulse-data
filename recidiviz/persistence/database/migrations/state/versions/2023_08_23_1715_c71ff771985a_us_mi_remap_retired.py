# pylint: skip-file
"""us_mi_remap_retired

Revision ID: c71ff771985a
Revises: 2c2c615a995f
Create Date: 2023-08-23 17:15:13.370850

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c71ff771985a"
down_revision = "2c2c615a995f"
branch_labels = None
depends_on = None

# Remap retired for employment periods
def upgrade() -> None:
    op.execute(
        """
        UPDATE state_employment_period 
        SET employment_status = 'ALTERNATE_INCOME_SOURCE'
        WHERE 
            state_code = 'US_MI' 
            and employment_status_raw_text in ('5105', 'RETIRED');
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_employment_period 
        SET employment_status = 'INTERNAL_UNKNOWN'
        WHERE 
            state_code = 'US_MI' 
            and employment_status_raw_text in ('RETIRED');

        UPDATE state_employment_period 
        SET employment_status = 'UNEMPLOYED'
        WHERE 
            state_code = 'US_MI' 
            and employment_status_raw_text in ('5105');
        """
    )
