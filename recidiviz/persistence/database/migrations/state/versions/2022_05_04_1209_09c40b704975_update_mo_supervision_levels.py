# pylint: skip-file
"""update_mo_supervision_levels

Revision ID: 09c40b704975
Revises: dbf7688df462
Create Date: 2022-05-04 12:09:41.695167

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "09c40b704975"
down_revision = "dbf7688df462"
branch_labels = None
depends_on = None

UNASSIGNED_UPGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'UNASSIGNED'
WHERE state_code = 'US_MO'
  AND supervision_level_raw_text IN ('IAP', 'NSV', 'PSI')
"""

UNASSIGNED_DOWNGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_MO'
  AND supervision_level_raw_text IN ('IAP', 'NSV', 'PSI')
"""

ELECTRONIC_MONITORING_ONLY_UPGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'ELECTRONIC_MONITORING_ONLY'
WHERE state_code = 'US_MO'
  AND supervision_level_raw_text = 'EMP'
"""

ELECTRONIC_MONITORING_ONLY_DOWNGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_MO'
  AND supervision_level_raw_text = 'EMP'
"""

MINIMUM_UPGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'MINIMUM'
WHERE state_code = 'US_MO'
  AND supervision_level_raw_text IN ('NOI', 'MIN', 'MSC')
"""

MINIMUM_DOWNGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_MO'
  AND supervision_level_raw_text IN ('NOI', 'MIN', 'MSC')
"""

MEDIUM_UPGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'MEDIUM'
WHERE state_code = 'US_MO'
  AND supervision_level_raw_text IN ('NII', 'REG')
"""

MEDIUM_DOWNGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_MO'
  AND supervision_level_raw_text IN ('NII', 'REG')
"""

HIGH_UPGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'HIGH'
WHERE state_code = 'US_MO'
  AND supervision_level_raw_text = 'ENH'
"""

HIGH_DOWNGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_MO'
  AND supervision_level_raw_text  = 'ENH'
"""

MAXIMUM_UPGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'MAXIMUM'
WHERE state_code = 'US_MO'
  AND supervision_level_raw_text IN ('CPP', 'RTF', 'VCT')
"""

MAXIMUM_DOWNGRADE_QUERY = """UPDATE {table_name}
SET
  supervision_level = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_MO'
  AND supervision_level_raw_text  IN ('CPP', 'RTF', 'VCT')
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
                    UNASSIGNED_UPGRADE_QUERY,
                    table_name=table_name,
                )
            )
            op.execute(
                StrictStringFormatter().format(
                    ELECTRONIC_MONITORING_ONLY_UPGRADE_QUERY,
                    table_name=table_name,
                )
            )
            op.execute(
                StrictStringFormatter().format(
                    MINIMUM_UPGRADE_QUERY,
                    table_name=table_name,
                )
            )
            op.execute(
                StrictStringFormatter().format(
                    MEDIUM_UPGRADE_QUERY,
                    table_name=table_name,
                )
            )
            op.execute(
                StrictStringFormatter().format(
                    HIGH_UPGRADE_QUERY,
                    table_name=table_name,
                )
            )
            op.execute(
                StrictStringFormatter().format(
                    MAXIMUM_UPGRADE_QUERY,
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
                    UNASSIGNED_DOWNGRADE_QUERY,
                    table_name=table_name,
                )
            )
            op.execute(
                StrictStringFormatter().format(
                    ELECTRONIC_MONITORING_ONLY_DOWNGRADE_QUERY,
                    table_name=table_name,
                )
            )
            op.execute(
                StrictStringFormatter().format(
                    MINIMUM_DOWNGRADE_QUERY,
                    table_name=table_name,
                )
            )
            op.execute(
                StrictStringFormatter().format(
                    MEDIUM_DOWNGRADE_QUERY,
                    table_name=table_name,
                )
            )
            op.execute(
                StrictStringFormatter().format(
                    HIGH_DOWNGRADE_QUERY,
                    table_name=table_name,
                )
            )
            op.execute(
                StrictStringFormatter().format(
                    MAXIMUM_DOWNGRADE_QUERY,
                    table_name=table_name,
                )
            )
