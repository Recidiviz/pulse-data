# pylint: skip-file
"""update_tn_supervision_level_mapping

Revision ID: 8825a4e6e583
Revises: 30bcc98c3784
Create Date: 2022-05-17 16:43:00.711958

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "8825a4e6e583"
down_revision = "30bcc98c3784"
branch_labels = None
depends_on = None


INTERSTATE_COMPACT_UPGRADE_QUERY = f"""UPDATE {{table_name}}
SET
  supervision_level = 'INTERSTATE_COMPACT'
WHERE state_code = 'US_TN'
  AND supervision_level_raw_text = '9IS'
"""

INTERSTATE_COMPACT_DOWNGRADE_QUERY = f"""UPDATE {{table_name}}
SET
  supervision_level = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_TN'
  AND supervision_level_raw_text = '9IS'
"""

IN_CUSTODY_UPGRADE_QUERY = f"""UPDATE {{table_name}}
SET
  supervision_level = 'IN_CUSTODY'
WHERE state_code = 'US_TN'
  AND supervision_level_raw_text = '9DT'
"""

IN_CUSTODY_DOWNGRADE_QUERY = f"""UPDATE {{table_name}}
SET
  supervision_level = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_TN'
  AND supervision_level_raw_text = '9DT'
"""

SUPERVISION_PERIOD_TABLE_NAME = "state_supervision_period"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for table_name in [
            SUPERVISION_PERIOD_TABLE_NAME,
        ]:
            op.execute(
                StrictStringFormatter().format(
                    INTERSTATE_COMPACT_UPGRADE_QUERY,
                    table_name=table_name,
                )
            )
            op.execute(
                StrictStringFormatter().format(
                    IN_CUSTODY_UPGRADE_QUERY,
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
                    INTERSTATE_COMPACT_DOWNGRADE_QUERY,
                    table_name=table_name,
                )
            )
            op.execute(
                StrictStringFormatter().format(
                    IN_CUSTODY_DOWNGRADE_QUERY,
                    table_name=table_name,
                )
            )
