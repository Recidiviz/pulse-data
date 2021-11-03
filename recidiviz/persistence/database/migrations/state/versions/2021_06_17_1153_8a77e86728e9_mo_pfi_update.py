# pylint: skip-file
"""mo_pfi_update

Revision ID: 8a77e86728e9
Revises: 8a50c937d9cb
Create Date: 2021-06-17 11:53:44.318674

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "8a77e86728e9"
down_revision = "8a50c937d9cb"
branch_labels = None
depends_on = None


UPDATE_QUERY = (
    "UPDATE {table_name} SET specialized_purpose_for_incarceration = {new_value}"
    " WHERE state_code = 'US_MO'"
    " AND specialized_purpose_for_incarceration_raw_text = 'S';"
)


INCARCERATION_PERIOD_TABLE_NAME = "state_incarceration_period"
INCARCERATION_PERIOD_HISTORY_TABLE_NAME = "state_incarceration_period_history"


def upgrade() -> None:
    connection = op.get_bind()

    new_value = "'GENERAL'"

    with op.get_context().autocommit_block():
        for table_name in [
            INCARCERATION_PERIOD_TABLE_NAME,
            INCARCERATION_PERIOD_HISTORY_TABLE_NAME,
        ]:
            connection.execute(
                StrictStringFormatter().format(
                    UPDATE_QUERY,
                    table_name=table_name,
                    new_value=new_value,
                )
            )


def downgrade() -> None:
    connection = op.get_bind()

    new_value = "NULL"

    with op.get_context().autocommit_block():
        for table_name in [
            INCARCERATION_PERIOD_TABLE_NAME,
            INCARCERATION_PERIOD_HISTORY_TABLE_NAME,
        ]:
            connection.execute(
                StrictStringFormatter().format(
                    UPDATE_QUERY,
                    table_name=table_name,
                    new_value=new_value,
                )
            )
