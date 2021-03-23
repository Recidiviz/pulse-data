# pylint: skip-file
"""migrate_to_transfer_out_of_state

Revision ID: e650b61844c5
Revises: 98cfa7151385
Create Date: 2021-03-22 12:02:15.296805

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e650b61844c5"
down_revision = "98cfa7151385"
branch_labels = None
depends_on = None


TRANSFERRED_OUT_OF_STATE_RELEASE_REASON_QUERY = (
    "SELECT incarceration_period_id FROM"
    " {table_name}"
    " WHERE release_reason = 'TRANSFERRED_OUT_OF_STATE'"
)

TRANSFERRED_OUT_OF_STATE_PROJECTED_RELEASE_REASON_QUERY = (
    "SELECT incarceration_period_id FROM"
    " {table_name}"
    " WHERE projected_release_reason = 'TRANSFERRED_OUT_OF_STATE'"
)

TRANSFER_OUT_OF_STATE_RELEASE_REASON_QUERY = (
    "SELECT incarceration_period_id FROM"
    " {table_name}"
    " WHERE release_reason = 'TRANSFER_OUT_OF_STATE'"
)

TRANSFER_OUT_OF_STATE_PROJECTED_RELEASE_QUERY = (
    "SELECT incarceration_period_id FROM"
    " {table_name}"
    " WHERE projected_release_reason = 'TRANSFER_OUT_OF_STATE'"
)


UPDATE_QUERY_RELEASE_REASON = (
    "UPDATE {table_name} SET release_reason = '{new_value}'"
    " WHERE incarceration_period_id IN ({ids_query});"
)

UPDATE_QUERY_PROJECTED_RELEASE_REASON = (
    "UPDATE {table_name} SET projected_release_reason = '{new_value}'"
    " WHERE incarceration_period_id IN ({ids_query});"
)

INCARCERATION_PERIOD_TABLE_NAME = "state_incarceration_period"
INCARCERATION_PERIOD_HISTORY_TABLE_NAME = "state_incarceration_period_history"


def upgrade():
    connection = op.get_bind()

    new_transfer_out_of_state = "TRANSFER_OUT_OF_STATE"

    connection.execute(
        UPDATE_QUERY_RELEASE_REASON.format(
            table_name=INCARCERATION_PERIOD_TABLE_NAME,
            new_value=new_transfer_out_of_state,
            ids_query=TRANSFERRED_OUT_OF_STATE_RELEASE_REASON_QUERY.format(
                table_name=INCARCERATION_PERIOD_TABLE_NAME
            ),
        )
    )

    connection.execute(
        UPDATE_QUERY_PROJECTED_RELEASE_REASON.format(
            table_name=INCARCERATION_PERIOD_TABLE_NAME,
            new_value=new_transfer_out_of_state,
            ids_query=TRANSFERRED_OUT_OF_STATE_PROJECTED_RELEASE_REASON_QUERY.format(
                table_name=INCARCERATION_PERIOD_TABLE_NAME
            ),
        )
    )

    connection.execute(
        UPDATE_QUERY_RELEASE_REASON.format(
            table_name=INCARCERATION_PERIOD_HISTORY_TABLE_NAME,
            new_value=new_transfer_out_of_state,
            ids_query=TRANSFERRED_OUT_OF_STATE_RELEASE_REASON_QUERY.format(
                table_name=INCARCERATION_PERIOD_HISTORY_TABLE_NAME
            ),
        )
    )

    connection.execute(
        UPDATE_QUERY_PROJECTED_RELEASE_REASON.format(
            table_name=INCARCERATION_PERIOD_HISTORY_TABLE_NAME,
            new_value=new_transfer_out_of_state,
            ids_query=TRANSFERRED_OUT_OF_STATE_PROJECTED_RELEASE_REASON_QUERY.format(
                table_name=INCARCERATION_PERIOD_HISTORY_TABLE_NAME
            ),
        )
    )


def downgrade():
    connection = op.get_bind()

    deprecated_transferred_out_of_state = "TRANSFERRED_OUT_OF_STATE"

    connection.execute(
        UPDATE_QUERY_RELEASE_REASON.format(
            table_name=INCARCERATION_PERIOD_TABLE_NAME,
            new_value=deprecated_transferred_out_of_state,
            ids_query=TRANSFER_OUT_OF_STATE_RELEASE_REASON_QUERY.format(
                table_name=INCARCERATION_PERIOD_TABLE_NAME
            ),
        )
    )

    connection.execute(
        UPDATE_QUERY_PROJECTED_RELEASE_REASON.format(
            table_name=INCARCERATION_PERIOD_TABLE_NAME,
            new_value=deprecated_transferred_out_of_state,
            ids_query=TRANSFER_OUT_OF_STATE_PROJECTED_RELEASE_QUERY.format(
                table_name=INCARCERATION_PERIOD_TABLE_NAME
            ),
        )
    )

    connection.execute(
        UPDATE_QUERY_RELEASE_REASON.format(
            table_name=INCARCERATION_PERIOD_HISTORY_TABLE_NAME,
            new_value=deprecated_transferred_out_of_state,
            ids_query=TRANSFER_OUT_OF_STATE_RELEASE_REASON_QUERY.format(
                table_name=INCARCERATION_PERIOD_HISTORY_TABLE_NAME
            ),
        )
    )

    connection.execute(
        UPDATE_QUERY_PROJECTED_RELEASE_REASON.format(
            table_name=INCARCERATION_PERIOD_HISTORY_TABLE_NAME,
            new_value=deprecated_transferred_out_of_state,
            ids_query=TRANSFER_OUT_OF_STATE_PROJECTED_RELEASE_QUERY.format(
                table_name=INCARCERATION_PERIOD_HISTORY_TABLE_NAME
            ),
        )
    )
