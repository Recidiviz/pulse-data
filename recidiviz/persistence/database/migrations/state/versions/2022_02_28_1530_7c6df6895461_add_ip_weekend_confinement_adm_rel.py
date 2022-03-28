# pylint: skip-file
"""add_ip_weekend_confinement_adm_rel

Revision ID: 7c6df6895461
Revises: f459d62ed8cb
Create Date: 2022-02-28 15:30:29.579031

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "7c6df6895461"
down_revision = "f459d62ed8cb"
branch_labels = None
depends_on = None


ADMISSION_REASON_UPGRADE_QUERY = """UPDATE {table_name} 
SET
  admission_reason = 'TEMPORARY_CUSTODY'
WHERE state_code = 'US_TN' 
  AND admission_reason_raw_text IN ('PRFA-WKEND','CCFA-WKEND');
"""

RELEASE_REASON_UPGRADE_QUERY = """UPDATE {table_name} 
SET
  release_reason = 'RELEASED_FROM_TEMPORARY_CUSTODY'
WHERE state_code = 'US_TN' 
  AND release_reason_raw_text IN ('FAPR-WKRET','FACC-WKRET');
"""


ADMISSION_REASON_DOWNGRADE_QUERY = """UPDATE {table_name} 
SET
  admission_reason = 'SANCTION_ADMISSION'
WHERE state_code = 'US_TN' 
  AND admission_reason_raw_text IN ('PRFA-WKEND','CCFA-WKEND');
"""

RELEASE_REASON_DOWNGRADE_QUERY = """UPDATE {table_name} 
SET
  release_reason = 'RELEASED_TO_SUPERVISION'
WHERE state_code = 'US_TN' 
  AND release_reason_raw_text IN ('FAPR-WKRET','FACC-WKRET');
"""

INCARCERATION_PERIOD_TABLE_NAME = "state_incarceration_period"
INCARCERATION_PERIOD_HISTORY_TABLE_NAME = "state_incarceration_period_history"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for table_name in [
            INCARCERATION_PERIOD_TABLE_NAME,
            INCARCERATION_PERIOD_HISTORY_TABLE_NAME,
        ]:
            for update_query in [
                ADMISSION_REASON_UPGRADE_QUERY,
                RELEASE_REASON_UPGRADE_QUERY,
            ]:
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
            for update_query in [
                ADMISSION_REASON_DOWNGRADE_QUERY,
                RELEASE_REASON_DOWNGRADE_QUERY,
            ]:
                op.execute(
                    StrictStringFormatter().format(
                        update_query,
                        table_name=table_name,
                    )
                )
