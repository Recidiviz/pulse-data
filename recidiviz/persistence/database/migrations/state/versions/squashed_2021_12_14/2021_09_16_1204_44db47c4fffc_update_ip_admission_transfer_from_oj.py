# pylint: skip-file
"""update_ip_admission_transfer_from_oj

Revision ID: 44db47c4fffc
Revises: 4dcecf6f3aea
Create Date: 2021-09-16 12:02:55.113264

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "44db47c4fffc"
down_revision = "4dcecf6f3aea"
branch_labels = None
depends_on = None


ENUM_VALUE_SELECT_IDS_QUERY = (
    "SELECT incarceration_period_id FROM {table_name}"
    " WHERE admission_reason = '{enum_value}'"
)

UPDATE_QUERY = (
    "UPDATE {table_name} SET admission_reason = '{new_value}'"
    " WHERE incarceration_period_id IN ({ids_query});"
)


INCARCERATION_PERIOD_TABLE_NAME = "state_incarceration_period"
INCARCERATION_PERIOD_HISTORY_TABLE_NAME = "state_incarceration_period_history"


def upgrade() -> None:
    connection = op.get_bind()

    old_value = "TRANSFERRED_FROM_OUT_OF_STATE"
    new_value = "TRANSFER_FROM_OTHER_JURISDICTION"

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
                    ids_query=StrictStringFormatter().format(
                        ENUM_VALUE_SELECT_IDS_QUERY,
                        table_name=table_name,
                        enum_value=old_value,
                    ),
                )
            )


def downgrade() -> None:
    connection = op.get_bind()

    old_value = "TRANSFER_FROM_OTHER_JURISDICTION"
    new_value = "TRANSFERRED_FROM_OUT_OF_STATE"

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
                    ids_query=StrictStringFormatter().format(
                        ENUM_VALUE_SELECT_IDS_QUERY,
                        table_name=table_name,
                        enum_value=old_value,
                    ),
                )
            )
