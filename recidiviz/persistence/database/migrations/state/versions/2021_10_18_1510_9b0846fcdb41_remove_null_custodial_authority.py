# pylint: skip-file
"""remove_null_custodial_authority

Revision ID: 9b0846fcdb41
Revises: 69d481f0a061
Create Date: 2021-10-18 15:10:08.842505

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9b0846fcdb41"
down_revision = "69d481f0a061"
branch_labels = None
depends_on = None

DELETE_QUERY = (
    "DELETE FROM {table_name}"
    " WHERE custodial_authority_raw_text IS NULL"
    " AND state_code = 'US_PA';"
)

TABLES_TO_UPDATE = [
    # Must delete from history table first to avoid violating foreign key constraint
    "state_incarceration_period_history",
    "state_incarceration_period",
]


def upgrade() -> None:
    for table in TABLES_TO_UPDATE:
        op.execute(DELETE_QUERY.format(table_name=table))


def downgrade() -> None:
    # Row deletion cannot be undone
    pass
