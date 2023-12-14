# pylint: skip-file
"""us_tn_remap_bench_warrant

Revision ID: a0fd7f2facdc
Revises: f578db7fbd32
Create Date: 2023-12-14 18:58:51.952507

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a0fd7f2facdc"
down_revision = "f578db7fbd32"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_type = 'WARRANT_STATUS'
        WHERE
            state_code = 'US_TN'
            AND supervision_type = 'BENCH_WARRANT';
            
        UPDATE state_supervision_period
        SET supervision_level = 'WARRANT'
        WHERE
            state_code = 'US_TN'
            and supervision_level_raw_text in ('9WR', 'WRT', 'ZWS');
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_type = 'BENCH_WARRANT'
        WHERE
            state_code = 'US_TN'
            AND supervision_type = 'WARRANT_STATUS';
            
        UPDATE state_supervision_period
        SET supervision_level = 'INTERNAL_UNKNOWN'
        WHERE
            state_code = 'US_TN'
            and supervision_level_raw_text in ('9WR', 'WRT', 'ZWS');
        """
    )
