# pylint: skip-file
"""remote_raw_data

Revision ID: cb0390d41f48
Revises: 1ee7fb62abf1
Create Date: 2023-02-10 11:12:30.412930

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "cb0390d41f48"
down_revision = "1ee7fb62abf1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """UPDATE direct_ingest_sftp_ingest_ready_file_metadata SET post_processed_normalized_file_path = REPLACE(post_processed_normalized_file_path, 'raw_data/', '');"""
    )


def downgrade() -> None:
    op.execute(
        """UPDATE direct_ingest_sftp_ingest_ready_file_metadata SET post_processed_normalized_file_path = CONCAT('raw_data/', post_processed_normalized_file_path);"""
    )
