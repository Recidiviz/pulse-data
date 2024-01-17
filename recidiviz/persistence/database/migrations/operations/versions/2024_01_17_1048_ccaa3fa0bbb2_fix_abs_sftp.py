# pylint: skip-file
"""fix_abs_sftp

Revision ID: ccaa3fa0bbb2
Revises: 7bebee78e7b8
Create Date: 2024-01-17 10:48:38.584127

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ccaa3fa0bbb2"
down_revision = "7bebee78e7b8"
branch_labels = None
depends_on = None

UPDATE_QUERY = """
UPDATE direct_ingest_sftp_remote_file_metadata SET remote_file_path = LTRIM(remote_file_path, '.') WHERE remote_file_path LIKE './%';
"""

DOWNGRADE_QUERY = """
UPDATE direct_ingest_sftp_remote_file_metadata SET remote_file_path = CONCAT('.', remote_file_path) WHERE remote_file_path NOT LIKE './%';
"""


def upgrade() -> None:
    op.execute(UPDATE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
