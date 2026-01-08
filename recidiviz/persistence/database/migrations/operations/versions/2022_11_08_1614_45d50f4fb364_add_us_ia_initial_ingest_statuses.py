# pylint: skip-file
"""add_us_ia_initial_ingest_statuses

Revision ID: 45d50f4fb364
Revises: 062929cfbc66
Create Date: 2022-11-08 16:14:04.519223

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "45d50f4fb364"
down_revision = "062929cfbc66"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO direct_ingest_instance_status (region_code, timestamp, instance, status) VALUES
        ('US_IA', '2022-11-08T00:00:00.000000', 'PRIMARY', 'STANDARD_RERUN_STARTED'),
        ('US_IA', '2022-11-08T00:00:00.000000', 'SECONDARY', 'NO_RERUN_IN_PROGRESS');
    """
    )


def downgrade() -> None:
    op.execute(
        """
           DELETE FROM direct_ingest_instance_status
           WHERE region_code = 'US_IA';
       """
    )
