# pylint: skip-file
"""me_admitted_term

Revision ID: 300c33c7fa5d
Revises: b7494deb434b
Create Date: 2022-07-13 11:19:05.013728

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "300c33c7fa5d"
down_revision = "b7494deb434b"
branch_labels = None
depends_on = None


UPGRADE_QUERY = """
UPDATE state_supervision_period SET termination_reason = 'ADMITTED_TO_INCARCERATION' WHERE state_code = 
'US_ME' AND SPLIT_PART(termination_reason_raw_text, '@@', 3) IN ('2', '7') 
AND SPLIT_PART(termination_reason_raw_text, '@@', 1) 
IN ('INCARCERATED', 'COUNTY JAIL', 'INTERSTATE COMPACT IN', 'PARTIAL REVOCATION - INCARCERATED');
"""

DOWNGRADE_QUERY = """
UPDATE state_supervision_period SET termination_reason = 'RETURN_TO_INCARCERATION' WHERE state_code = 
'US_ME' AND SPLIT_PART(termination_reason_raw_text, '@@', 3) IN ('2', '7') 
AND SPLIT_PART(termination_reason_raw_text, '@@', 1) 
IN ('INCARCERATED', 'COUNTY JAIL', 'INTERSTATE COMPACT IN', 'PARTIAL REVOCATION - INCARCERATED');
"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
