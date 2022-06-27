# pylint: skip-file
"""tn_add_absconsion_sup_type

Revision ID: 51d22e2b9b32
Revises: dabe8760362d
Create Date: 2022-06-27 15:42:48.080120

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "51d22e2b9b32"
down_revision = "dabe8760362d"
branch_labels = None
depends_on = None

ABSCONSION_TYPE_UPGRADE_QUERY = f"""UPDATE state_supervision_period
SET
  supervision_type = 'ABSCONSION'
WHERE state_code = 'US_TN'
  AND supervision_level_raw_text IN ('9AB', 'ZAB', 'ZAC', 'ZAP')
"""

BENCH_WARRANT_TYPE_UPGRADE_QUERY = f"""UPDATE state_supervision_period
SET
  supervision_type = 'BENCH_WARRANT'
WHERE state_code = 'US_TN'
  AND supervision_level_raw_text IN ('9WR', 'NIA', 'WRB', 'WRT', 'ZWS')
"""


def upgrade() -> None:
    op.execute(StrictStringFormatter().format(ABSCONSION_TYPE_UPGRADE_QUERY))
    op.execute(StrictStringFormatter().format(BENCH_WARRANT_TYPE_UPGRADE_QUERY))


def downgrade() -> None:
    # This migration does not have a downgrade
    pass
