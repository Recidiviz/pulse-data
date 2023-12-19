# pylint: skip-file
"""us_mi_supervision_level

Revision ID: 06ac42365c32
Revises: 2071ae47b07f
Create Date: 2023-05-30 15:56:06.505502

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "06ac42365c32"
down_revision = "2071ae47b07f"
branch_labels = None
depends_on = None


# In cases where custodial authority is OTHER_STATE and supervision level is currently NULL,
# set supervision level to INTERNAL_UNKNOWN (see #20972)
def upgrade() -> None:
    """
    UPDATE state_supervision_period
    SET supervision_level = 'INTERNAL_UNKNOWN'
    WHERE state_code = 'US_MI'
    AND custodial_authority = 'OTHER_STATE'
    AND supervision_level_raw_text is NULL
    AND supervision_level is NULL;
    """


# For the downgrade, set supervision level back to NULL if custodial authority is OTHER_STATE
# and supervision level was not set to INTERNAL UNKNOWN due to raw text
def downgrade() -> None:
    """
    UPDATE state_supervision_period
    SET supervision_level = NULL
    WHERE state_code = 'US_MI'
    AND custodial_authority = 'OTHER_STATE'
    AND supervision_level_raw_text is NULL
    AND supervision_level = 'INTERNAL_UNKNOWN';
    """
