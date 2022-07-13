# pylint: skip-file
"""id_admitted_term

Revision ID: b7494deb434b
Revises: f929dea1fa43
Create Date: 2022-07-13 11:11:11.326382

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b7494deb434b"
down_revision = "f929dea1fa43"
branch_labels = None
depends_on = None


UPGRADE_QUERY = """
UPDATE state_supervision_period SET termination_reason = 'ADMITTED_TO_INCARCERATION' WHERE state_code = 
'US_ID' AND termination_reason_raw_text IN ('I', 'O');
"""

DOWNGRADE_QUERY = """
UPDATE state_supervision_period SET termination_reason = 'RETURN_TO_INCARCERATION' WHERE state_code = 
'US_ID' AND termination_reason_raw_text IN ('I', 'O');
"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
