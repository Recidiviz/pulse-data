# pylint: skip-file
"""term_dismissed_to_vacated_2

Revision ID: 83bdd9b935c4
Revises: 7f7182f42a83
Create Date: 2022-07-12 15:50:17.653272

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "83bdd9b935c4"
down_revision = "7f7182f42a83"
branch_labels = None
depends_on = None

UPGRADE_QUERY = """
UPDATE state_supervision_period SET termination_reason = 'VACATED' WHERE state_code = 
'US_ID' AND termination_reason_raw_text LIKE '%PROBATION DISMISSED/VACANT%';"""

DOWNGRADE_QUERY = """
UPDATE state_supervision_period SET termination_reason = 'DISMISSED' WHERE state_code = 
'US_ID' AND termination_reason_raw_text LIKE '%PROBATION DISMISSED/VACANT%';
"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
