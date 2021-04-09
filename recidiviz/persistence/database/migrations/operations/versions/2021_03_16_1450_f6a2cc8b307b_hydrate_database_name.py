# pylint: skip-file
"""hydrate_database_name

Revision ID: f6a2cc8b307b
Revises: 1f6edf08e547
Create Date: 2021-03-16 14:50:45.670077

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f6a2cc8b307b"
down_revision = "1f6edf08e547"
branch_labels = None
depends_on = None

UPGRADE_QUERY = """
UPDATE direct_ingest_ingest_file_metadata
SET ingest_database_name = 'postgres'
WHERE ingest_database_name IS NULL;
"""

DOWNGRADE_QUERY = """
UPDATE direct_ingest_ingest_file_metadata
SET ingest_database_name = NULL
WHERE ingest_database_name = 'postgres';
"""


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(DOWNGRADE_QUERY)
