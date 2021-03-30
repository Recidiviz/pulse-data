# pylint: skip-file
"""us_id_update_sp_termination_reason_mappings

Revision ID: 6e69681e48da
Revises: 3913de7c14eb
Create Date: 2021-03-24 14:26:51.411080

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "6e69681e48da"
down_revision = "b4e453c8363f"
branch_labels = None
depends_on = None


TERMINATION_REASON_QUERY = (
    "SELECT supervision_period_id FROM"
    " {table_name}"
    " WHERE termination_reason_raw_text LIKE '%%DECEASED%%' AND state_code = 'US_ID'"
)


UPDATE_QUERY = (
    "UPDATE {table_name} SET termination_reason = '{new_value}'"
    " WHERE supervision_period_id IN ({ids_query});"
)

SUPERVISION_PERIOD_TABLE_NAME = "state_supervision_period"
SUPERVISION_PERIOD_HISTORY_TABLE_NAME = "state_supervision_period_history"


def upgrade():
    updated_termination_reason = "DEATH"
    with op.get_context().autocommit_block():
        for table_name in [
            SUPERVISION_PERIOD_TABLE_NAME,
            SUPERVISION_PERIOD_HISTORY_TABLE_NAME,
        ]:
            op.execute(
                UPDATE_QUERY.format(
                    table_name=table_name,
                    new_value=updated_termination_reason,
                    ids_query=TERMINATION_REASON_QUERY.format(table_name=table_name),
                )
            )


def downgrade():
    updated_termination_reason = "EXPIRATION"
    with op.get_context().autocommit_block():
        for table_name in [
            SUPERVISION_PERIOD_TABLE_NAME,
            SUPERVISION_PERIOD_HISTORY_TABLE_NAME,
        ]:
            op.execute(
                UPDATE_QUERY.format(
                    table_name=table_name,
                    new_value=updated_termination_reason,
                    ids_query=TERMINATION_REASON_QUERY.format(table_name=table_name),
                )
            )
