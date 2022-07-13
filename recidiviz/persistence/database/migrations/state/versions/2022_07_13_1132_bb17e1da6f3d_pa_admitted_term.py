# pylint: skip-file
"""pa_admitted_term

Revision ID: bb17e1da6f3d
Revises: 300c33c7fa5d
Create Date: 2022-07-13 11:32:47.993162

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "bb17e1da6f3d"
down_revision = "300c33c7fa5d"
branch_labels = None
depends_on = None


UPGRADE_QUERY = """
UPDATE state_supervision_period SET termination_reason = 'ADMITTED_TO_INCARCERATION' WHERE state_code = 
'US_PA' AND termination_reason_raw_text = '44';
"""

DOWNGRADE_QUERY = """
UPDATE state_supervision_period SET termination_reason = 'RETURN_TO_INCARCERATION' WHERE state_code = 
'US_PA' AND termination_reason_raw_text = '44';
"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
