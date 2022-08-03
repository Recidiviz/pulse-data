# pylint: skip-file
"""tn_cust_auth_county

Revision ID: 27ce610daa5e
Revises: 7e4b2632d898
Create Date: 2022-08-01 14:21:40.060248

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "27ce610daa5e"
down_revision = "7e4b2632d898"
branch_labels = None
depends_on = None

UPGRADE_INCARCERATION_QUERY = """
UPDATE state_incarceration_period SET custodial_authority = 'COUNTY' WHERE state_code = 'US_TN' 
AND (SPLIT_PART(custodial_authority_raw_text, '-', 1) = 'JA' OR 
SPLIT_PART(custodial_authority_raw_text, '-', 2) IN ('019', '033', '046', '047', '054', 
'057', '075', '079', '082'));
"""

DOWNGRADE_INCARCERATION_QUERY = """
UPDATE state_incarceration_period SET custodial_authority = 'COURT' WHERE state_code = 'US_TN' 
AND (SPLIT_PART(custodial_authority_raw_text, '-', 1) = 'JA' OR 
SPLIT_PART(custodial_authority_raw_text, '-', 2) IN ('019', '033', '046', '047', '054', 
'057', '075', '079', '082'));
"""


def upgrade() -> None:
    op.execute(UPGRADE_INCARCERATION_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_INCARCERATION_QUERY)
