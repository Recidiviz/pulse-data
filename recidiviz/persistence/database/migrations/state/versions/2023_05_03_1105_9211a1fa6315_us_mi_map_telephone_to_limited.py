# pylint: skip-file
"""us_mi_map_telephone_to_limited

Revision ID: 9211a1fa6315
Revises: 3560a9ea5c84
Create Date: 2023-05-03 11:05:26.938223

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9211a1fa6315"
down_revision = "3560a9ea5c84"
branch_labels = None
depends_on = None

# We want to revise the mapping of these four supervision levels from MINIMUM to LIMITED
# "14165" (Probation Minimum Telephone Employed)
# "14164" (Probation Minimum Telephone Unemployed)
# "14052" (Parole Minimum Telephone Unemployed)
# "14051" (Parole Minimum Telephone Employed)


def upgrade() -> None:
    """
    UPDATE state_supervision_period
    SET supervision_level = 'LIMITED'
    WHERE state_code = 'US_MI'
      AND supervision_level_raw_text in ("14165","14164","14052","14051")
    """


def downgrade() -> None:
    """
    UPDATE state_supervision_period
    SET supervision_level = 'MINIMUM'
    WHERE state_code = 'US_MI'
      AND supervision_level_raw_text in ("14165","14164","14052","14051")
    """
