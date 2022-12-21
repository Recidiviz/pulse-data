# pylint: skip-file
"""clear_release_reason_raw_text_pa

Revision ID: 1374407a9dbd
Revises: b2a714862a82
Create Date: 2022-12-21 15:41:56.646462

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1374407a9dbd"
down_revision = "b2a714862a82"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_period
        SET release_reason_raw_text = NULL 
        WHERE release_date is NULL
        AND release_reason_raw_text IS NOT NULL
        AND state_code = 'US_PA'
        """
    )
    pass


def downgrade() -> None:
    pass
