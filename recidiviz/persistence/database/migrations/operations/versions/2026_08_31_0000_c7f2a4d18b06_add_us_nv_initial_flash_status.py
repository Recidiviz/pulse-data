# pylint: skip-file
"""add_us_nv_initial_flash_status

Revision ID: c7f2a4d18b06
Revises: b8e4c1a09f23
Create Date: 2026-08-31 00:00:00.000000

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "c7f2a4d18b06"
down_revision = "b8e4c1a09f23"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO direct_ingest_raw_data_flash_status (region_code, status_timestamp, flashing_in_progress) VALUES
        ('US_NV', '2026-08-31 00:00:00.000000', '0');
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DELETE FROM direct_ingest_raw_data_flash_status WHERE region_code = 'US_NV';
        """
    )
