# pylint: skip-file
"""add_us_ny_initial_statuses

Revision ID: a1b2c3d4e5f6
Revises: 3fb951a6ae34
Create Date: 2025-08-14 22:08:50.827585

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "3fb951a6ae34"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO direct_ingest_raw_data_flash_status (region_code, status_timestamp, flashing_in_progress) VALUES
        ('US_NY', '2025-08-14 22:08:50.827585', '0');
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DELETE FROM direct_ingest_raw_data_flash_status WHERE region_code = 'US_NY';
        """
    )
