# pylint: skip-file
"""drop_us_mo_ssvr

Revision ID: 787c13afabf2
Revises: 8431180b6a66
Create Date: 2021-08-17 16:49:22.563764

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "787c13afabf2"
down_revision = "8431180b6a66"
branch_labels = None
depends_on = None

UPDATE_QUERY = (
    "UPDATE {table_name} SET source_supervision_violation_response_id = NULL"
    " WHERE state_code IN ('US_MO');"
)

TABLES_TO_UPDATE = ["state_incarceration_period", "state_incarceration_period_history"]


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for table in TABLES_TO_UPDATE:
            op.execute(UPDATE_QUERY.format(table_name=table))


def downgrade() -> None:
    # Field deprecation cannot be undone
    pass
