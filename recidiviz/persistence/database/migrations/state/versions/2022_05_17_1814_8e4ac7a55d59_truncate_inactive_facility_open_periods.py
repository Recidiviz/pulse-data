# pylint: skip-file
"""truncate_inactive_facility_open_periods

Revision ID: 8e4ac7a55d59
Revises: 8825a4e6e583
Create Date: 2022-05-17 18:14:29.640476

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "8e4ac7a55d59"
down_revision = "8825a4e6e583"
branch_labels = None
depends_on = None

KCSC_NCSC_MCRC_MTRC_BMSP_UPGRADE_QUERY = """UPDATE state_incarceration_period
SET
  release_date = '1997-12-31', release_reason = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_TN'
  AND release_date IS NULL AND facility IN ('KCSC','NCSC','MCRC','MTRC','BMSP')
"""

TSPR_DBCI_UPGRADE_QUERY = """UPDATE state_incarceration_period
SET
  release_date = '1992-06-30', release_reason = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_TN'
  AND release_date IS NULL AND facility IN ('TSPR','DBCI')
"""

TPFW_UPGRADE_QUERY = """UPDATE state_incarceration_period
SET
  release_date = '2020-08-31', release_reason = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_TN'
  AND release_date IS NULL AND facility = 'TPFW'
"""


def upgrade() -> None:
    op.execute(StrictStringFormatter().format(KCSC_NCSC_MCRC_MTRC_BMSP_UPGRADE_QUERY))
    op.execute(StrictStringFormatter().format(TSPR_DBCI_UPGRADE_QUERY))
    op.execute(StrictStringFormatter().format(TPFW_UPGRADE_QUERY))


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
