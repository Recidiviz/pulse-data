# pylint: skip-file
"""return_to_admitted_from_supervision

Revision ID: 2f89a19901c3
Revises: 6b86cdd713ab
Create Date: 2021-04-15 12:30:50.724732

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "2f89a19901c3"
down_revision = "6b86cdd713ab"
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

    old_value = "RETURN_FROM_SUPERVISION"
    new_value = "ADMITTED_FROM_SUPERVISION"

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

    old_value = "ADMITTED_FROM_SUPERVISION"
    new_value = "RETURN_FROM_SUPERVISION"

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
