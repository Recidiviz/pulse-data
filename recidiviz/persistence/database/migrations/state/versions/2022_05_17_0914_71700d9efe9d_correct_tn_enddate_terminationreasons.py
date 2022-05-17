# pylint: skip-file
"""correct_tn_enddate_terminationreasons

Revision ID: 71700d9efe9d
Revises: d4b8d47ed4ef
Create Date: 2022-05-17 09:14:01.235890

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "71700d9efe9d"
down_revision = "d4b8d47ed4ef"
branch_labels = None
depends_on = None

TERMINATIONREASON_UPGRADE_QUERY = """UPDATE {table_name}
SET
  termination_reason_raw_text = NULL, termination_reason = NULL
WHERE state_code = 'US_TN'
  AND termination_date IS NULL AND termination_reason_raw_text = 'TRANS'
"""

TERMINATIONREASON_DOWNGRADE_QUERY = """UPDATE {table_name}
SET
  termination_reason_raw_text = 'TRANS', termination_reason = 'TRANSFER_WITHIN_STATE'
WHERE state_code = 'US_TN'
  AND termination_date IS NULL AND termination_reason_raw_text IS NULL
"""

SUPERVISION_PERIOD_TABLE_NAME = "state_supervision_period"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for table_name in [
            SUPERVISION_PERIOD_TABLE_NAME,
        ]:
            op.execute(
                StrictStringFormatter().format(
                    TERMINATIONREASON_UPGRADE_QUERY,
                    table_name=table_name,
                )
            )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for table_name in [
            SUPERVISION_PERIOD_TABLE_NAME,
        ]:
            op.execute(
                StrictStringFormatter().format(
                    TERMINATIONREASON_DOWNGRADE_QUERY,
                    table_name=table_name,
                )
            )
