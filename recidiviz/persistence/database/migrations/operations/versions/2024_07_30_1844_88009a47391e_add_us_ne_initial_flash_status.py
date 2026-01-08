# pylint: skip-file
"""add_us_ne_initial_flash_status

Revision ID: 88009a47391e
Revises: d429094710bb
Create Date: 2024-07-30 18:44:43.542319

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "88009a47391e"
down_revision = "d429094710bb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
                INSERT INTO direct_ingest_raw_data_flash_status (region_code, status_timestamp, flashing_in_progress) VALUES
                ('US_NE', '2024-07-30T00:00:00.000000', '0')
            """
    )


def downgrade() -> None:
    op.execute(
        """
                   DELETE FROM direct_ingest_raw_data_flash_status
                   WHERE region_code = 'US_NE';
               """
    )
