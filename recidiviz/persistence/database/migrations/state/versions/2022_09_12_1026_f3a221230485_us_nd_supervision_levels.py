# pylint: skip-file
"""us_nd_supervision_levels

Revision ID: f3a221230485
Revises: 244b56c165ac
Create Date: 2022-09-12 10:26:39.489212

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f3a221230485"
down_revision = "244b56c165ac"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'INTERNAL_UNKNOWN'
        WHERE supervision_level_raw_text = '4'
        AND state_code = 'US_ND'
        """
    )
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'UNASSIGNED'
        WHERE supervision_level_raw_text = '5'
        AND state_code = 'US_ND'
        """
    )
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'INTERNAL_UNKNOWN'
        WHERE supervision_level_raw_text = '8'
        AND state_code = 'US_ND'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'EXTERNAL_UNKNOWN'
        WHERE supervision_level_raw_text = '4'
        AND state_code = 'US_ND'
        """
    )
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'EXTERNAL_UNKNOWN'
        WHERE supervision_level_raw_text = '5'
        AND state_code = 'US_ND'
        """
    )
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'EXTERNAL_UNKNOWN'
        WHERE supervision_level_raw_text = '8'
        AND state_code = 'US_ND'
        """
    )
