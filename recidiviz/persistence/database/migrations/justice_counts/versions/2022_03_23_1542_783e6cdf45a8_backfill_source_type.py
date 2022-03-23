# pylint: skip-file
"""backfill source type

Revision ID: 783e6cdf45a8
Revises: 71fb90aaa368
Create Date: 2022-03-23 15:42:16.534228

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "783e6cdf45a8"
down_revision = "71fb90aaa368"
branch_labels = None
depends_on = None


UPDATE_QUERY = "UPDATE source SET type = 'source';"
DOWNGRADE_QUERY = "UPDATE source SET type = NULL;"


def upgrade() -> None:
    connection = op.get_bind()
    connection.execute(UPDATE_QUERY)


def downgrade() -> None:
    connection = op.get_bind()
    connection.execute(DOWNGRADE_QUERY)
