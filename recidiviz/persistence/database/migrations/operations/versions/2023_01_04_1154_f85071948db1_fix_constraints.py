# pylint: skip-file
"""fix_constraints

Revision ID: f85071948db1
Revises: 3b6259ffb36d
Create Date: 2023-01-04 11:54:40.323571

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f85071948db1"
down_revision = "3b6259ffb36d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_check_constraint(
        "nonnull_sftp_ingest_ready_file_discovery_time",
        "direct_ingest_sftp_ingest_ready_file_metadata",
        "file_discovery_time IS NOT NULL",
    )
    op.create_check_constraint(
        "nonnull_sftp_remote_file_discovery_time",
        "direct_ingest_sftp_remote_file_metadata",
        "file_discovery_time IS NOT NULL",
    )
    op.create_check_constraint(
        "nonnull_sftp_timestamp",
        "direct_ingest_sftp_remote_file_metadata",
        "sftp_timestamp IS NOT NULL",
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(
        "nonnull_sftp_ingest_ready_file_discovery_time",
        "direct_ingest_sftp_ingest_ready_file_metadata",
    )
    op.drop_constraint(
        "nonnull_sftp_remote_file_discovery_time",
        "direct_ingest_sftp_remote_file_metadata",
    )
    op.drop_constraint(
        "nonnull_sftp_timestamp", "direct_ingest_sftp_remote_file_metadata"
    )
    # ### end Alembic commands ###
