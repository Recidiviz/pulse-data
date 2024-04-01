# pylint: skip-file
"""remove_legacy_ingest_statuses

Revision ID: 54384bcdcbb0
Revises: 5fcde57de6f0
Create Date: 2024-03-27 16:46:59.632923

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "54384bcdcbb0"
down_revision = "5fcde57de6f0"
branch_labels = None
depends_on = None

# Without new value
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
    "BLOCKED_ON_PRIMARY_RAW_DATA_IMPORT",
    "RERUN_CANCELED",
    "RERUN_CANCELLATION_IN_PROGRESS",
    "RAW_DATA_REIMPORT_STARTED",
    "INITIAL_STATE",
    "RAW_DATA_REIMPORT_CANCELED",
    "RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS",
    "RAW_DATA_UP_TO_DATE",
    "NO_RAW_DATA_REIMPORT_IN_PROGRESS",
]

# With new value
new_values = [
    "RAW_DATA_IMPORT_IN_PROGRESS",
    "READY_TO_FLASH",
    "FLASH_IN_PROGRESS",
    "FLASH_COMPLETED",
    "STALE_RAW_DATA",
    "RAW_DATA_REIMPORT_STARTED",
    "INITIAL_STATE",
    "RAW_DATA_REIMPORT_CANCELED",
    "RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS",
    "RAW_DATA_UP_TO_DATE",
    "NO_RAW_DATA_REIMPORT_IN_PROGRESS",
]


def upgrade() -> None:
    statuses_to_delete = (
        "RERUN_WITH_RAW_DATA_IMPORT_STARTED",
        "STANDARD_RERUN_STARTED",
        "INGEST_VIEW_MATERIALIZATION_IN_PROGRESS",
        "EXTRACT_AND_MERGE_IN_PROGRESS",
        "UP_TO_DATE",
        "NO_RERUN_IN_PROGRESS",
        "BLOCKED_ON_PRIMARY_RAW_DATA_IMPORT",
        "RERUN_CANCELED",
        "RERUN_CANCELLATION_IN_PROGRESS",
    )

    op.execute(
        f"DELETE FROM direct_ingest_instance_status "
        f"WHERE status IN {statuses_to_delete};"
    )

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
