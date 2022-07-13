# pylint: skip-file
"""term_dismissed_to_vacated

Revision ID: 7f7182f42a83
Revises: e6ed1b4205e5
Create Date: 2022-07-12 15:34:49.045469

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7f7182f42a83"
down_revision = "e6ed1b4205e5"
branch_labels = None
depends_on = None

UPGRADE_QUERY = """
UPDATE state_supervision_period SET termination_reason = 'VACATED' WHERE state_code = 
'US_TN' AND termination_reason_raw_text = 'RVR';
"""

DOWNGRADE_QUERY = """
UPDATE state_supervision_period SET termination_reason = 'DISMISSED' WHERE state_code = 
'US_TN' AND termination_reason_raw_text = 'RVR';
"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
