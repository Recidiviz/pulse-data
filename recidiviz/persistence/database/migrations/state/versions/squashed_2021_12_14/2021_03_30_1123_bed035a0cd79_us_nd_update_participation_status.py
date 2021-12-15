# pylint: skip-file
"""us_nd_update_participation_status

Revision ID: bed035a0cd79
Revises: 26f50952aaab
Create Date: 2021-03-30 11:23:44.284084

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "bed035a0cd79"
down_revision = "26f50952aaab"
branch_labels = None
depends_on = None


REFUSED_STATUS_QUERY = (
    "SELECT external_id FROM"
    " {table_name}"
    " WHERE participation_status_raw_text LIKE '%%REFUSED%%' AND state_code = 'US_ND'"
)


UPDATE_QUERY = (
    "UPDATE {table_name} SET participation_status = '{new_value}'"
    " WHERE external_id IN ({ids_query});"
)

PROGRAM_ASSIGNMENT_TABLE_NAME = "state_program_assignment"
PROGRAM_ASSIGNMENT_HISTORY_TABLE_NAME = "state_program_assignment_history"


def upgrade() -> None:
    updated_participation_status = "REFUSED"
    with op.get_context().autocommit_block():
        for table_name in [
            PROGRAM_ASSIGNMENT_TABLE_NAME,
            PROGRAM_ASSIGNMENT_HISTORY_TABLE_NAME,
        ]:
            op.execute(
                StrictStringFormatter().format(
                    UPDATE_QUERY,
                    table_name=table_name,
                    new_value=updated_participation_status,
                    ids_query=StrictStringFormatter().format(
                        REFUSED_STATUS_QUERY, table_name=table_name
                    ),
                )
            )


def downgrade() -> None:
    updated_participation_status = "EXTERNAL_UNKNOWN"
    with op.get_context().autocommit_block():
        for table_name in [
            PROGRAM_ASSIGNMENT_TABLE_NAME,
            PROGRAM_ASSIGNMENT_HISTORY_TABLE_NAME,
        ]:
            op.execute(
                StrictStringFormatter().format(
                    UPDATE_QUERY,
                    table_name=table_name,
                    new_value=updated_participation_status,
                    ids_query=StrictStringFormatter().format(
                        REFUSED_STATUS_QUERY, table_name=table_name
                    ),
                )
            )
