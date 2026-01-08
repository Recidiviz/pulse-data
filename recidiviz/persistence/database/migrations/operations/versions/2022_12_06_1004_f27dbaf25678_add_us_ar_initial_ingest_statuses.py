# pylint: skip-file
"""add_us_ar_initial_ingest_statuses

Revision ID: f27dbaf25678
Revises: bc4309eb8002
Create Date: 2022-12-06 10:04:39.649504

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f27dbaf25678"
down_revision = "bc4309eb8002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO direct_ingest_instance_status (region_code, timestamp, instance, status) VALUES
        ('US_AR', '2022-12-05T00:00:00.000000', 'PRIMARY', 'STANDARD_RERUN_STARTED'),
        ('US_AR', '2022-12-05T00:00:00.000000', 'SECONDARY', 'NO_RERUN_IN_PROGRESS');
    """
    )


def downgrade() -> None:
    op.execute(
        """
            DELETE FROM direct_ingest_instance_status
            WHERE region_code = 'US_AR';
        """
    )
