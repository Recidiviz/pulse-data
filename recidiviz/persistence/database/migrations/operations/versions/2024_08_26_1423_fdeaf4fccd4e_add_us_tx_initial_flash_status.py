# pylint: skip-file
"""add_us_tx_initial_flash_status

Revision ID: fdeaf4fccd4e
Revises: de369946ba7d
Create Date: 2024-08-26 14:23:26.092980

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "fdeaf4fccd4e"
down_revision = "de369946ba7d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
                INSERT INTO direct_ingest_raw_data_flash_status (region_code, status_timestamp, flashing_in_progress) VALUES
                ('US_TX', '2024-08-26T00:00:00.000000', '0')
            """
    )


def downgrade() -> None:
    op.execute(
        """
                   DELETE FROM direct_ingest_raw_data_flash_status
                   WHERE region_code = 'US_TX';
               """
    )
