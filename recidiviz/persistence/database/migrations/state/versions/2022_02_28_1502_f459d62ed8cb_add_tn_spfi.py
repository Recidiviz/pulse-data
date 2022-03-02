# pylint: skip-file
"""add_tn_spfi

Revision ID: f459d62ed8cb
Revises: 5e8a10a5f4f8
Create Date: 2022-02-28 15:02:01.487054

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "f459d62ed8cb"
down_revision = "5e8a10a5f4f8"
branch_labels = None
depends_on = None


WEEKEND_UPGRADE_QUERY = """UPDATE {table_name} 
SET
  specialized_purpose_for_incarceration_raw_text = admission_reason_raw_text,
  specialized_purpose_for_incarceration = 'WEEKEND_CONFINEMENT' 
WHERE state_code = 'US_TN' 
  AND admission_reason_raw_text IN ('PRFA-WKEND','CCFA-WKEND');
"""

GENERAL_UPGRADE_QUERY = """UPDATE {table_name} 
SET
  specialized_purpose_for_incarceration_raw_text = admission_reason_raw_text,
  specialized_purpose_for_incarceration = 'GENERAL' 
WHERE state_code = 'US_TN' 
  AND admission_reason_raw_text NOT IN ('PRFA-WKEND','CCFA-WKEND');
"""

DOWNGRADE_QUERY = """UPDATE {table_name} 
SET
  specialized_purpose_for_incarceration_raw_text = NULL,
  specialized_purpose_for_incarceration = NULL 
WHERE state_code = 'US_TN';
"""

INCARCERATION_PERIOD_TABLE_NAME = "state_incarceration_period"
INCARCERATION_PERIOD_HISTORY_TABLE_NAME = "state_incarceration_period"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for table_name in [
            INCARCERATION_PERIOD_TABLE_NAME,
            INCARCERATION_PERIOD_HISTORY_TABLE_NAME,
        ]:
            for update_query in [WEEKEND_UPGRADE_QUERY, GENERAL_UPGRADE_QUERY]:
                op.execute(
                    StrictStringFormatter().format(
                        update_query,
                        table_name=table_name,
                    )
                )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for table_name in [
            INCARCERATION_PERIOD_TABLE_NAME,
            INCARCERATION_PERIOD_HISTORY_TABLE_NAME,
        ]:
            op.execute(
                StrictStringFormatter().format(
                    DOWNGRADE_QUERY,
                    table_name=table_name,
                )
            )
