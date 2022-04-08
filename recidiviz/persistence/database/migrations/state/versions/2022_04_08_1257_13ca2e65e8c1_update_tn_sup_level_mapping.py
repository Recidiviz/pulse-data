# pylint: skip-file
"""update_tn_sup_level_mapping

Revision ID: 13ca2e65e8c1
Revises: bb7c46572952
Create Date: 2022-04-08 12:57:45.182529

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "13ca2e65e8c1"
down_revision = "bb7c46572952"
branch_labels = None
depends_on = None

UNSUPERVISED_UPGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'UNSUPERVISED'
WHERE state_code = 'US_TN'
  AND supervision_level_raw_text IN ('9JS', '9SD', 'SDS')
"""

UNSUPERVISED_DOWNGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_TN'
  AND supervision_level_raw_text IN ('9JS', '9SD', 'SDS')
"""

MEDIUM_UPGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'MEDIUM'
WHERE state_code = 'US_TN'
  AND supervision_level_raw_text = '6P3'
"""

MEDIUM_DOWNGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_TN'
  AND supervision_level_raw_text = '6P3'
"""

SUPERVISION_PERIOD_TABLE_NAME = "state_supervision_period"
SUPERVISION_PERIOD_HISTORY_TABLE_NAME = "state_supervision_period_history"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for table_name in [
            SUPERVISION_PERIOD_TABLE_NAME,
            SUPERVISION_PERIOD_HISTORY_TABLE_NAME,
        ]:
            op.execute(
                StrictStringFormatter().format(
                    UNSUPERVISED_UPGRADE_QUERY,
                    table_name=table_name,
                )
            )
            op.execute(
                StrictStringFormatter().format(
                    MEDIUM_UPGRADE_QUERY,
                    table_name=table_name,
                )
            )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for table_name in [
            SUPERVISION_PERIOD_TABLE_NAME,
            SUPERVISION_PERIOD_HISTORY_TABLE_NAME,
        ]:
            op.execute(
                StrictStringFormatter().format(
                    UNSUPERVISED_DOWNGRADE_QUERY,
                    table_name=table_name,
                )
            )
            op.execute(
                StrictStringFormatter().format(
                    MEDIUM_DOWNGRADE_QUERY,
                    table_name=table_name,
                )
            )
