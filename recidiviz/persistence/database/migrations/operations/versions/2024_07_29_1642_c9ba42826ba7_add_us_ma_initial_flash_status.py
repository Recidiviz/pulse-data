# pylint: skip-file
"""add_us_ma_initial_flash_status

Revision ID: c9ba42826ba7
Revises: 1399435b05bb
Create Date: 2024-07-29 16:42:11.813322

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c9ba42826ba7"
down_revision = "1399435b05bb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
                INSERT INTO direct_ingest_raw_data_flash_status (region_code, status_timestamp, flashing_in_progress) VALUES
                ('US_MA', '2024-07-29T00:00:00.000000', '0')
            """
    )


def downgrade() -> None:
    op.execute(
        """
                   DELETE FROM direct_ingest_raw_data_flash_status
                   WHERE region_code = 'US_MA';
               """
    )
