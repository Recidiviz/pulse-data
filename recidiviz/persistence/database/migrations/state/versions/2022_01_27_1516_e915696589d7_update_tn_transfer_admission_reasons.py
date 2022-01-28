# pylint: skip-file
"""update_tn_transfer_admission_reasons

Changes incarceration period admission_reason from NEW_ADMISSION to TRANSFER for two specific raw text values.
Applies this change in both state_incarceration_period and state_incarceration_period_history.

Revision ID: e915696589d7
Revises: 9d865823db99
Create Date: 2022-01-27 15:16:26.093391

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "e915696589d7"
down_revision = "9d865823db99"
branch_labels = None
depends_on = None


UPGRADE_QUERY = (
    "UPDATE {table_name} SET admission_reason = 'TRANSFER'"
    " WHERE state_code = 'US_TN' and admission_reason_raw_text IN ('CTFA-RTCHG', 'CTFA-RETNO');"
)


DOWNGRADE_QUERY = (
    "UPDATE {table_name} SET admission_reason = 'NEW_ADMISSION'"
    " WHERE state_code = 'US_TN' and admission_reason_raw_text IN ('CTFA-RTCHG', 'CTFA-RETNO');"
)


TABLE_NAMES = ["state_incarceration_period", "state_incarceration_period_history"]


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
