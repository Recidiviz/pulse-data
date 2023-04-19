# pylint: skip-file
"""remove-flash-cancel-enums

Revision ID: cacbd74da5a3
Revises: 31f7930a2b21
Create Date: 2023-04-19 11:52:40.373541

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "cacbd74da5a3"
down_revision = "31f7930a2b21"
branch_labels = None
depends_on = None

# With `FLASH_CANCELED` and `FLASH_CANCELLATION_IN_PROGRESS`
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
    "FLASH_CANCELED",
    "FLASH_CANCELLATION_IN_PROGRESS",
    "RERUN_CANCELED",
    "RERUN_CANCELLATION_IN_PROGRESS",
]

# Without `FLASH_CANCELED` and `FLASH_CANCELLATION_IN_PROGRESS`
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
    "BLOCKED_ON_PRIMARY_RAW_DATA_IMPORT",
    "RERUN_CANCELED",
    "RERUN_CANCELLATION_IN_PROGRESS",
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
