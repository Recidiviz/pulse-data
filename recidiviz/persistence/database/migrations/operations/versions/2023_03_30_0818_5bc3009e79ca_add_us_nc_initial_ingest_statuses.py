# pylint: skip-file
"""add_us_nc_initial_ingest_statuses

Revision ID: 5bc3009e79ca
Revises: 242464cb6b60
Create Date: 2023-03-30 08:18:34.557896

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "5bc3009e79ca"
down_revision = "242464cb6b60"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO direct_ingest_instance_status (region_code, instance, status, status_timestamp) VALUES
        ('US_NC', 'PRIMARY', 'STANDARD_RERUN_STARTED', '2023-03-30T00:00:00.000000'),
        ('US_NC', 'SECONDARY', 'NO_RERUN_IN_PROGRESS', '2023-03-30T00:00:00.000000');
    """
    )


def downgrade() -> None:
    op.execute(
        """
           DELETE FROM direct_ingest_instance_status
           WHERE region_code = 'US_NC';
       """
    )
