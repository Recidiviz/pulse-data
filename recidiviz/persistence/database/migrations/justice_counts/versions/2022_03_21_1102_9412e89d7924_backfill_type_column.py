# pylint: skip-file
"""backfill type column

Revision ID: 9412e89d7924
Revises: d50163044ba2
Create Date: 2022-03-21 11:02:34.832855

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9412e89d7924"
down_revision = "d50163044ba2"
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
