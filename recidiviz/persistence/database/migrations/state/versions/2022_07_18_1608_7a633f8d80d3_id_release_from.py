# pylint: skip-file
"""id_release_from

Revision ID: 7a633f8d80d3
Revises: 0614af617346
Create Date: 2022-07-18 16:08:21.694543

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7a633f8d80d3"
down_revision = "0614af617346"
branch_labels = None
depends_on = None


UPGRADE_QUERY = """
UPDATE state_supervision_period SET admission_reason = 'RELEASE_FROM_INCARCERATION' WHERE state_code = 
'US_ID' AND admission_reason_raw_text IN ('I', 'O');
"""

DOWNGRADE_QUERY = """
UPDATE state_supervision_period SET admission_reason = 'CONDITIONAL_RELEASE' WHERE state_code = 
'US_ID' AND admission_reason_raw_text IN ('I', 'O');
"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
