# pylint: skip-file
"""change_raw_text_tn

Revision ID: e2a5093ac219
Revises: 5d754ee809f6
Create Date: 2021-12-15 13:18:00.505039

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "e2a5093ac219"
down_revision = "5d754ee809f6"
branch_labels = None
depends_on = None

ENUM_VALUE_SELECT_IDS_QUERY = (
    "SELECT incarceration_period_id FROM {table_name}"
    " WHERE state_code = 'US_TN' and release_reason_raw_text = {raw_text_value}"
)

UPDATE_QUERY = (
    "UPDATE {table_name} SET release_reason_raw_text = {raw_text_value}"
    " WHERE state_code = 'US_TN' and incarceration_period_id IN ({ids_query});"
)

TABLE_NAMES = ["state_incarceration_period", "state_incarceration_period_history"]


def upgrade() -> None:
    for table_name in TABLE_NAMES:
        op.execute(
            StrictStringFormatter().format(
                UPDATE_QUERY,
                table_name=table_name,
                raw_text_value="NULL",
                ids_query=StrictStringFormatter().format(
                    ENUM_VALUE_SELECT_IDS_QUERY,
                    table_name=table_name,
                    raw_text_value="'NONE-NONE'",
                ),
            )
        )


def downgrade() -> None:
    for table_name in TABLE_NAMES:
        op.execute(
            StrictStringFormatter().format(
                UPDATE_QUERY,
                table_name=table_name,
                raw_text_value="'NONE-NONE'",
                ids_query=StrictStringFormatter().format(
                    ENUM_VALUE_SELECT_IDS_QUERY,
                    table_name=table_name,
                    raw_text_value="NULL",
                ),
            )
        )
