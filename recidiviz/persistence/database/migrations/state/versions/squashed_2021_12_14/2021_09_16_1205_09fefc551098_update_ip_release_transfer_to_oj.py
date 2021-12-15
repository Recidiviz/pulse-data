# pylint: skip-file
"""update_ip_release_transfer_to_oj

Revision ID: 09fefc551098
Revises: 44db47c4fffc
Create Date: 2021-09-16 12:03:30.264086

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "09fefc551098"
down_revision = "44db47c4fffc"
branch_labels = None
depends_on = None


ENUM_VALUE_SELECT_IDS_QUERY = (
    "SELECT incarceration_period_id FROM {table_name}"
    " WHERE {column_name} = '{enum_value}'"
)

UPDATE_QUERY = (
    "UPDATE {table_name} SET {column_name} = '{new_value}'"
    " WHERE incarceration_period_id IN ({ids_query});"
)

COLUMNS_TO_UPDATE = ["release_reason", "projected_release_reason"]
INCARCERATION_PERIOD_TABLE_NAME = "state_incarceration_period"
INCARCERATION_PERIOD_HISTORY_TABLE_NAME = "state_incarceration_period_history"


def upgrade() -> None:
    connection = op.get_bind()

    old_value = "TRANSFER_OUT_OF_STATE"
    new_value = "TRANSFER_TO_OTHER_JURISDICTION"

    with op.get_context().autocommit_block():
        for table_name in [
            INCARCERATION_PERIOD_TABLE_NAME,
            INCARCERATION_PERIOD_HISTORY_TABLE_NAME,
        ]:
            for column in COLUMNS_TO_UPDATE:
                connection.execute(
                    StrictStringFormatter().format(
                        UPDATE_QUERY,
                        table_name=table_name,
                        column_name=column,
                        new_value=new_value,
                        ids_query=StrictStringFormatter().format(
                            ENUM_VALUE_SELECT_IDS_QUERY,
                            table_name=table_name,
                            column_name=column,
                            enum_value=old_value,
                        ),
                    )
                )

                connection.execute(
                    StrictStringFormatter().format(
                        UPDATE_QUERY,
                        table_name=table_name,
                        column_name=column,
                        new_value=new_value,
                        ids_query=StrictStringFormatter().format(
                            ENUM_VALUE_SELECT_IDS_QUERY,
                            table_name=table_name,
                            column_name=column,
                            enum_value=old_value,
                        ),
                    )
                )


def downgrade() -> None:
    connection = op.get_bind()

    old_value = "TRANSFER_TO_OTHER_JURISDICTION"
    new_value = "TRANSFER_OUT_OF_STATE"

    with op.get_context().autocommit_block():
        for table_name in [
            INCARCERATION_PERIOD_TABLE_NAME,
            INCARCERATION_PERIOD_HISTORY_TABLE_NAME,
        ]:
            for column in COLUMNS_TO_UPDATE:
                connection.execute(
                    StrictStringFormatter().format(
                        UPDATE_QUERY,
                        table_name=table_name,
                        column_name=column,
                        new_value=new_value,
                        ids_query=StrictStringFormatter().format(
                            ENUM_VALUE_SELECT_IDS_QUERY,
                            table_name=table_name,
                            column_name=column,
                            enum_value=old_value,
                        ),
                    )
                )

                connection.execute(
                    StrictStringFormatter().format(
                        UPDATE_QUERY,
                        table_name=table_name,
                        column_name=column,
                        new_value=new_value,
                        ids_query=StrictStringFormatter().format(
                            ENUM_VALUE_SELECT_IDS_QUERY,
                            table_name=table_name,
                            column_name=column,
                            enum_value=old_value,
                        ),
                    )
                )
