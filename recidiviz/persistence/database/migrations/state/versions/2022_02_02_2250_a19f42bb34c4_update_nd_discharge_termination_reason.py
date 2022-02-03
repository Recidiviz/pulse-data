# pylint: skip-file
"""update_nd_discharge_termination_reason

Revision ID: a19f42bb34c4
Revises: dfad4970cfdd
Create Date: 2022-02-02 22:50:36.779541

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "a19f42bb34c4"
down_revision = "dfad4970cfdd"
branch_labels = None
depends_on = None


UPGRADE_QUERY = (
    "UPDATE {table_name} SET termination_reason = 'DISCHARGE'"
    " WHERE state_code = 'US_ND' and termination_reason_raw_text = '29';"
)


DOWNGRADE_QUERY = (
    "UPDATE {table_name} SET admission_reason = 'INVESTIGATION'"
    " WHERE state_code = 'US_ND' and termination_reason_raw_text = '29';"
)


TABLE_NAMES = ["state_supervision_period", "state_supervision_period_history"]


def upgrade() -> None:
    for table_name in TABLE_NAMES:
        op.execute(
            StrictStringFormatter().format(
                UPGRADE_QUERY,
                table_name=table_name,
            )
        )


def downgrade() -> None:
    for table_name in TABLE_NAMES:
        op.execute(
            StrictStringFormatter().format(
                DOWNGRADE_QUERY,
                table_name=table_name,
            )
        )
