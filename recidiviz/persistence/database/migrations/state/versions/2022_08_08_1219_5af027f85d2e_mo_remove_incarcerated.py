# pylint: skip-file
"""mo_remove_incarcerated

Revision ID: 5af027f85d2e
Revises: fdca0d088c46
Create Date: 2022-08-08 12:19:42.093603

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5af027f85d2e"
down_revision = "fdca0d088c46"
branch_labels = None
depends_on = None

UPGRADE_QUERY = "UPDATE state_supervision_period SET supervision_level = 'IN_CUSTODY' WHERE state_code = 'US_MO' AND supervision_level_raw_text IN ('ITC', 'JAL', 'PRS', 'SHK');"
DOWNGRADE_QUERY = "UPDATE state_supervision_period SET supervision_level = 'INCARCERATED' WHERE state_code = 'US_MO' AND supervision_level_raw_text IN ('ITC', 'JAL', 'PRS', 'SHK');"


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
