# pylint: skip-file
"""add_us_nyc_initial_flash_status

Revision ID: b8e4c1a09f23
Revises: 0298c740be75
Create Date: 2026-08-26 00:00:00.000000

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "b8e4c1a09f23"
down_revision = "0298c740be75"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO direct_ingest_raw_data_flash_status (region_code, status_timestamp, flashing_in_progress) VALUES
        ('US_NYC', '2026-08-26 00:00:00.000000', '0');
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DELETE FROM direct_ingest_raw_data_flash_status WHERE region_code = 'US_NYC';
        """
    )
