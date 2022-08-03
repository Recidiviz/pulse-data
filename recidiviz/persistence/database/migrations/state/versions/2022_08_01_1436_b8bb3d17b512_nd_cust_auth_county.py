# pylint: skip-file
"""nd_cust_auth_county

Revision ID: b8bb3d17b512
Revises: 27ce610daa5e
Create Date: 2022-08-01 14:36:02.173271

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b8bb3d17b512"
down_revision = "27ce610daa5e"
branch_labels = None
depends_on = None


UPGRADE_INCARCERATION_QUERY = """
UPDATE state_incarceration_period SET custodial_authority = 'COUNTY' WHERE state_code = 'US_ND' 
AND SPLIT_PART(custodial_authority_raw_text, '-', 1) IN ('CJ', 'DEFP', 'NW', 'SC', 'SW');
"""

DOWNGRADE_INCARCERATION_QUERY = """
UPDATE state_incarceration_period SET custodial_authority = 'COURT' WHERE state_code = 'US_ND' 
AND SPLIT_PART(custodial_authority_raw_text, '-', 1) IN ('CJ', 'DEFP', 'NW', 'SC', 'SW');
"""


def upgrade() -> None:
    op.execute(UPGRADE_INCARCERATION_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_INCARCERATION_QUERY)
