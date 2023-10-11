# pylint: skip-file
"""us_mi_map_in_custody

Revision ID: 227714f1a0ff
Revises: e1fd9273a93c
Create Date: 2023-10-07 11:34:58.515441

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "227714f1a0ff"
down_revision = "e1fd9273a93c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period 
        SET supervision_level = 'IN_CUSTODY'
        WHERE 
            state_code = 'US_MI' 
            and supervision_level_raw_text like '%IN JAIL%';
        """
    )


def downgrade() -> None:

    op.execute(
        """
        UPDATE state_supervision_period 
        SET supervision_level = 'MAXIMUM'
        WHERE 
            state_code = 'US_MI' 
            and supervision_level_raw_text like '%IN JAIL%'
            and supervision_level_raw_text like 'MAXIMUM%';

        UPDATE state_supervision_period 
        SET supervision_level = 'MEDIUM'
        WHERE 
            state_code = 'US_MI' 
            and supervision_level_raw_text like '%IN JAIL%'
            and supervision_level_raw_text like 'MEDIUM%';

        UPDATE state_supervision_period 
        SET supervision_level = 'IN_CUSTODY'
        WHERE 
            state_code = 'US_MI' 
            and supervision_level_raw_text like '%IN JAIL%'
            and supervision_level_raw_text like 'MINIMUM ADMINISTRATIVE%';

        UPDATE state_supervision_period 
        SET supervision_level = 'INTERNAL_UNKNOWN'
        WHERE 
            state_code = 'US_MI' 
            and supervision_level_raw_text like '%IN JAIL%'
            and supervision_level_raw_text like 'NONE%';

        UPDATE state_supervision_period 
        SET supervision_level = 'MINIMUM'
        WHERE 
            state_code = 'US_MI' 
            and supervision_level_raw_text like '%IN JAIL%'
            and supervision_level_raw_text like 'MINIMUM%';

        UPDATE state_supervision_period 
        SET supervision_level = 'HIGH'
        WHERE
            state_code = 'US_MI' 
            and supervision_level_raw_text like '%IN JAIL%'
            and supervision_level_raw_text like 'INTENSIVE%';
        """
    )
