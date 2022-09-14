# pylint: skip-file
"""add_flash_cancellation_enum

Revision ID: 6656c163a20f
Revises: 0e66bcee6c96
Create Date: 2022-09-14 11:11:50.702349

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6656c163a20f"
down_revision = "0e66bcee6c96"
branch_labels = None
depends_on = None


# Without FLASH_CANCELLATION_IN_PROGRESS
old_values = [
    "RERUN_WITH_RAW_DATA_IMPORT_STARTED",
    "STANDARD_RERUN_STARTED",
    "RAW_DATA_IMPORT_IN_PROGRESS",
    "INGEST_VIEW_MATERIALIZATION_IN_PROGRESS",
    "EXTRACT_AND_MERGE_IN_PROGRESS",
    "READY_TO_FLASH",
    "FLASH_IN_PROGRESS",
    "FLASH_COMPLETED",
    "UP_TO_DATE",
    "STALE_RAW_DATA",
    "NO_RERUN_IN_PROGRESS",
    "FLASH_CANCELED",
]


# With FLASH_CANCELLATION_IN_PROGRESS
new_values = [
    "RERUN_WITH_RAW_DATA_IMPORT_STARTED",
    "STANDARD_RERUN_STARTED",
    "RAW_DATA_IMPORT_IN_PROGRESS",
    "INGEST_VIEW_MATERIALIZATION_IN_PROGRESS",
    "EXTRACT_AND_MERGE_IN_PROGRESS",
    "READY_TO_FLASH",
    "FLASH_IN_PROGRESS",
    "FLASH_COMPLETED",
    "UP_TO_DATE",
    "STALE_RAW_DATA",
    "NO_RERUN_IN_PROGRESS",
    "FLASH_CANCELED",
    "FLASH_CANCELLATION_IN_PROGRESS",
]


def upgrade() -> None:
    op.execute("ALTER TYPE direct_ingest_status RENAME TO direct_ingest_status_old;")
    sa.Enum(*new_values, name="direct_ingest_status").create(bind=op.get_bind())
    op.alter_column(
        "direct_ingest_instance_status",
        column_name="status",
        type_=sa.Enum(*new_values, name="direct_ingest_status"),
        postgresql_using="status::text::direct_ingest_status",
    )
    op.execute("DROP TYPE direct_ingest_status_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE direct_ingest_status RENAME TO direct_ingest_status_old;")
    sa.Enum(*old_values, name="direct_ingest_status").create(bind=op.get_bind())
    op.alter_column(
        "direct_ingest_instance_status",
        column_name="status",
        type_=sa.Enum(*old_values, name="direct_ingest_status"),
        postgresql_using="status::text::direct_ingest_status",
    )
    op.execute("DROP TYPE direct_ingest_status_old;")
