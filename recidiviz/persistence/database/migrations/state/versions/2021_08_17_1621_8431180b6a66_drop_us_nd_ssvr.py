# pylint: skip-file
"""drop_us_nd_ssvr

Revision ID: 8431180b6a66
Revises: 6b4aad103cc6
Create Date: 2021-08-17 16:21:28.526216

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8431180b6a66"
down_revision = "6b4aad103cc6"
branch_labels = None
depends_on = None

UPDATE_QUERY = (
    "UPDATE {table_name} SET source_supervision_violation_response_id = NULL"
    " WHERE state_code IN ('US_ND');"
)

TABLES_TO_UPDATE = ["state_incarceration_period", "state_incarceration_period_history"]


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for table in TABLES_TO_UPDATE:
            op.execute(UPDATE_QUERY.format(table_name=table))


def downgrade() -> None:
    # Field deprecation cannot be undone
    pass
