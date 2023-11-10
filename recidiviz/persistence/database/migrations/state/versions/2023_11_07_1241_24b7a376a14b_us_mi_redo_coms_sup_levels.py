# pylint: skip-file
"""us_mi_redo_coms_sup_levels

Revision ID: 24b7a376a14b
Revises: 8b3e4ce45334
Create Date: 2023-11-07 12:41:45.782473

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "24b7a376a14b"
down_revision = "8b3e4ce45334"
branch_labels = None
depends_on = None

# In MI, supervision level raw text from COMS data comes from two sources
# and so the raw text takes the format "{Supervision Level}_{Supervision Specialty}".
# Originally, we mapped supervision level based solely on {Supervision Level} if that was not NONE,
# and mapped supervision level based on {Supervision Specialty} if it was NONE.
# This migration revises supervision level to be mapped based on whichever supervision level value
# ({Supervision Level} or {Superviison Specialty}) indicates a more severe level.


def upgrade() -> None:
    # Update statements ordered by increasing level of severity so that the highest
    # severity level is prioritized
    op.execute(
        """
        UPDATE state_supervision_period 
        SET supervision_level = 'LIMITED'
        WHERE 
            state_code = 'US_MI' 
            and supervision_level_raw_text like '%MINIMUM TRS%';

        UPDATE state_supervision_period 
        SET supervision_level = 'MINIMUM'
        WHERE 
            state_code = 'US_MI' 
            and (supervision_level_raw_text like '%MINIMUM IN-PERSON%' or supervision_level_raw_text like '%MINIMUM LOW%');

        UPDATE state_supervision_period 
        SET supervision_level = 'MEDIUM'
        WHERE 
            state_code = 'US_MI' 
            and supervision_level_raw_text like '%MEDIUM%';

        UPDATE state_supervision_period 
        SET supervision_level = 'MAXIMUM'
        WHERE 
            state_code = 'US_MI' 
            and supervision_level_raw_text like '%MAXIMUM%';

        UPDATE state_supervision_period 
        SET supervision_level = 'HIGH'
        WHERE 
            state_code = 'US_MI' 
            and supervision_level_raw_text like '%INTENSIVE%';

        UPDATE state_supervision_period 
        SET supervision_level = 'WARRANT'
        WHERE 
            state_code = 'US_MI' 
            and supervision_level_raw_text like '%WARRANT%';

        UPDATE state_supervision_period 
        SET supervision_level = 'ABSCONSION'
        WHERE 
            state_code = 'US_MI' 
            and supervision_level_raw_text like '%ABSCONDER%';

        UPDATE state_supervision_period 
        SET supervision_level = 'IN_CUSTODY'
        WHERE 
            state_code = 'US_MI' 
            and (supervision_level_raw_text like '%IN JAIL%' or supervision_level_raw_text like '%MINIMUM ADMINISTRATIVE%');
        """
    )


def downgrade() -> None:
    # Update statements written in an order that reflects how mappings were originally prioritized.
    # Supervision level raw text from COMS would be of the format {supervision_level}_{supervision_specialty}
    # and originally, we would map based on {supervision_level} if it wasn't NONE.  And if {supervision_level} was NONE,
    # then we'd map based on {supervision_specialty}
    op.execute(
        """
        UPDATE state_supervision_period 
        SET supervision_level = 'WARRANT'
        WHERE 
            state_code = 'US_MI' 
            AND (
                SPLIT_PART(supervision_level_raw_text, '_', 1) like '%WARRANT%' OR 
                (supervision_level_raw_text like 'NONE_%' and supervision_level_raw_text like '%WARRANT%')
            );

        UPDATE state_supervision_period 
        SET supervision_level = 'ABSCONSION'
        WHERE 
            state_code = 'US_MI' 
            AND (
                SPLIT_PART(supervision_level_raw_text, '_', 1) like '%ABSCONDER%' OR 
                (supervision_level_raw_text like 'NONE_%' and supervision_level_raw_text like '%ABSCONDER%')
            );

        UPDATE state_supervision_period 
        SET supervision_level = 'HIGH'
        WHERE 
            state_code = 'US_MI' 
            AND (
                SPLIT_PART(supervision_level_raw_text, '_', 1) like '%INTENSIVE%' OR 
                (supervision_level_raw_text like 'NONE_%' and supervision_level_raw_text like '%INTENSIVE%')
            );

        UPDATE state_supervision_period 
        SET supervision_level = 'MAXIMUM'
        WHERE 
            state_code = 'US_MI' 
            AND (
                SPLIT_PART(supervision_level_raw_text, '_', 1) like '%MAXIMUM%' OR 
                (supervision_level_raw_text like 'NONE_%' and supervision_level_raw_text like '%MAXIMUM%')
            );

        UPDATE state_supervision_period 
        SET supervision_level = 'MEDIUM'
        WHERE 
            state_code = 'US_MI' 
            AND (
                SPLIT_PART(supervision_level_raw_text, '_', 1) like '%MEDIUM%' OR 
                (supervision_level_raw_text like 'NONE_%' and supervision_level_raw_text like '%MEDIUM%')
            );

        UPDATE state_supervision_period 
        SET supervision_level = 'MINIMUM'
        WHERE 
            state_code = 'US_MI' 
            AND (
                SPLIT_PART(supervision_level_raw_text, '_', 1) like '%MINIMUM%' OR 
                (supervision_level_raw_text like 'NONE_%' and supervision_level_raw_text like '%MINIMUM%')
            );

        UPDATE state_supervision_period 
        SET supervision_level = 'IN_CUSTODY'
        WHERE 
            state_code = 'US_MI' 
            AND (
                SPLIT_PART(supervision_level_raw_text, '_', 1) like '%MINIMUM ADMINISTRATIVE%' OR 
                (supervision_level_raw_text like 'NONE_%' and supervision_level_raw_text like '%MINIMUM ADMINISTRATIVE%')
            );

        UPDATE state_supervision_period 
        SET supervision_level = 'IN_CUSTODY'
        WHERE 
            state_code = 'US_MI' 
            AND supervision_level_raw_text like '%IN JAIL%';
        """
    )
