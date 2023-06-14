# pylint: skip-file
"""us_mi_supervision_level_and_proj_min_completion_date

Revision ID: ae6fcf44f068
Revises: 68e07d95ef0a
Create Date: 2023-06-14 12:06:44.359484

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ae6fcf44f068"
down_revision = "68e07d95ef0a"
branch_labels = None
depends_on = None

# This migration executes the following three separate migrations that written and merged but not properly executed:

# original revision = "9211a1fa6315"
# We want to revise the mapping of these four supervision levels from MINIMUM to LIMITED
# "14165" (Probation Minimum Telephone Employed)
# "14164" (Probation Minimum Telephone Unemployed)
# "14052" (Parole Minimum Telephone Unemployed)
# "14051" (Parole Minimum Telephone Employed)

# original revision = "4dd0c6a3ccfc"
# We want to set parole eligibility date to projected_min_release_date

# original revision = "06ac42365c32"
# In cases where custodial authority is OTHER_STATE and supervision level is currently NULL,
# set supervision level to INTERNAL_UNKNOWN (see #20972)


def upgrade() -> None:

    # original revision = "9211a1fa6315"
    op.execute(
        """
    UPDATE state_supervision_period
    SET supervision_level = 'LIMITED'
    WHERE state_code = 'US_MI'
    AND supervision_level_raw_text in ('14165','14164','14052','14051');
    """
    )

    # original revision = "4dd0c6a3ccfc"
    op.execute(
        """
    UPDATE state_incarceration_sentence
    SET projected_min_release_date = parole_eligibility_date
    WHERE state_code = 'US_MI';
    """
    )

    # original revision = "06ac42365c32"
    op.execute(
        """
    UPDATE state_supervision_period
    SET supervision_level = 'INTERNAL_UNKNOWN'
    WHERE state_code = 'US_MI'
    AND custodial_authority = 'OTHER_STATE'
    AND supervision_level_raw_text is NULL
    AND supervision_level is NULL;
    """
    )


def downgrade() -> None:

    # original revision = "9211a1fa6315"
    op.execute(
        """
    UPDATE state_supervision_period
    SET supervision_level = 'MINIMUM'
    WHERE state_code = 'US_MI'
    AND supervision_level_raw_text in ('14165','14164','14052','14051');
    """
    )

    # original revision = "4dd0c6a3ccfc"
    op.execute(
        """
    UPDATE state_incarceration_sentence
    SET projected_min_release_date = NULL
    WHERE state_code = 'US_MI';
    """
    )

    # original revision = "06ac42365c32"
    op.execute(
        """
    UPDATE state_supervision_period
    SET supervision_level = NULL
    WHERE state_code = 'US_MI'
    AND custodial_authority = 'OTHER_STATE'
    AND supervision_level_raw_text is NULL
    AND supervision_level = 'INTERNAL_UNKNOWN';
    """
    )
