# pylint: skip-file
"""backfill report status

Revision ID: d0d71bdbdd54
Revises: f5c597786912
Create Date: 2022-04-07 12:23:00.244939

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d0d71bdbdd54"
down_revision = "f5c597786912"
branch_labels = None
depends_on = None


UPDATE_QUERY = "UPDATE report SET status = 'PUBLISHED';"
DOWNGRADE_QUERY = "UPDATE report SET status = NULL;"


def upgrade() -> None:
    connection = op.get_bind()
    connection.execute(UPDATE_QUERY)


def downgrade() -> None:
    connection = op.get_bind()
    connection.execute(DOWNGRADE_QUERY)
