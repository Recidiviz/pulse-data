# pylint: skip-file
"""add_us_az_initial_ingest_statuses

Revision ID: 7bebee78e7b8
Revises: a8e2a82b2058
Create Date: 2023-11-22 15:40:39.023243

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7bebee78e7b8"
down_revision = "1274abfe52b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
                INSERT INTO direct_ingest_instance_status (region_code, instance, status, status_timestamp) VALUES
                ('US_AZ', 'PRIMARY', 'INITIAL_STATE', '2023-11-21T00:00:00.000000'),
                ('US_AZ', 'SECONDARY', 'NO_RAW_DATA_REIMPORT_IN_PROGRESS', '2023-11-21T00:00:00.000000');
            """
    )


def downgrade() -> None:
    op.execute(
        """
            DELETE FROM direct_ingest_instance_status
            WHERE region_code = 'US_AZ';
        """
    )
