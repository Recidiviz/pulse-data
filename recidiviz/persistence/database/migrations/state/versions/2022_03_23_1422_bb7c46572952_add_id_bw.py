# pylint: skip-file
"""add_id_bw
Revision ID: bb7c46572952
Revises: b024ac1a811d
Create Date: 2022-03-23 14:22:10.500892
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "bb7c46572952"
down_revision = "b024ac1a811d"
branch_labels = None
depends_on = None


UPGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_type = 'BENCH_WARRANT'
WHERE state_code = 'US_ID'
  AND supervision_type_raw_text = 'BW';
"""

DOWNGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_type = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_ID'
  AND supervision_type_raw_text = 'BW';
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
                    UPGRADE_QUERY,
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
                    DOWNGRADE_QUERY,
                    table_name=table_name,
                )
            )
