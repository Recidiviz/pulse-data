# pylint: skip-file
"""us_ix_bw_level

Revision ID: e284041ae6d4
Revises: 0fe793632366
Create Date: 2023-10-12 16:21:27.356370

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e284041ae6d4"
down_revision = "0fe793632366"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period 
        SET supervision_level = 'WARRANT'
        WHERE 
            state_code = 'US_IX' 
            and supervision_level_raw_text = 'BENCH WARRANT';
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period 
        SET supervision_level = 'INTERNAL_UNKNOWN'
        WHERE 
            state_code = 'US_IX' 
            and supervision_level_raw_text = 'BENCH WARRANT';
        """
    )
