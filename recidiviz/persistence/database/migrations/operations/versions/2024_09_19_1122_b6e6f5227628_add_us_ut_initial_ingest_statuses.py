# pylint: skip-file
"""add_us_ut_initial_ingest_statuses

Revision ID: b6e6f5227628
Revises: fdeaf4fccd4e
Create Date: 2024-09-19 11:22:58.299840

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "b6e6f5227628"
down_revision = "fdeaf4fccd4e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO direct_ingest_instance_status (region_code, instance, status, status_timestamp) VALUES
        ('US_UT', 'PRIMARY', 'INITIAL_STATE', '2024-09-19 11:22:58.299840'),
        ('US_UT', 'SECONDARY', 'NO_RAW_DATA_REIMPORT_IN_PROGRESS', '2024-09-19 11:22:58.299840');

        INSERT INTO direct_ingest_raw_data_flash_status (region_code, status_timestamp, flashing_in_progress) VALUES
        ('US_UT', '2024-09-19 11:22:58.299840', '0');
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DELETE FROM direct_ingest_raw_data_flash_status WHERE region_code = 'US_UT';
        DELETE FROM direct_ingest_instance_status WHERE region_code = 'US_UT';
        """
    )
