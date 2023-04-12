# pylint: skip-file
"""update_status_names

Revision ID: 31f7930a2b21
Revises: 15bcbab33ab1
Create Date: 2023-04-10 15:29:33.975670

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "31f7930a2b21"
down_revision = "15bcbab33ab1"
branch_labels = None
depends_on = None

UPGRADE_CANCELED_QUERY = """
UPDATE direct_ingest_instance_status
SET status = 'RERUN_CANCELED'
WHERE status = 'FLASH_CANCELED'
"""

DOWNGRADE_CANCELED_QUERY = """
UPDATE direct_ingest_instance_status
SET status = 'FLASH_CANCELED'
WHERE status = 'RERUN_CANCELED'
"""

UPGRADE_IN_PROGRESS_QUERY = """
UPDATE direct_ingest_instance_status
SET status = 'RERUN_CANCELLATION_IN_PROGRESS'
WHERE status = 'FLASH_CANCELLATION_IN_PROGRESS'
"""

DOWNGRADE_IN_PROGRESS_QUERY = """
UPDATE direct_ingest_instance_status
SET status = 'FLASH_CANCELLATION_IN_PROGRESS'
WHERE status = 'RERUN_CANCELLATION_IN_PROGRESS'
"""


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(UPGRADE_CANCELED_QUERY)
        op.execute(UPGRADE_IN_PROGRESS_QUERY)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(DOWNGRADE_CANCELED_QUERY)
        op.execute(DOWNGRADE_IN_PROGRESS_QUERY)
