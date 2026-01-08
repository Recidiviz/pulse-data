# pylint: skip-file
"""add_instance_status_or

Revision ID: 0e66bcee6c96
Revises: 52702703ae24
Create Date: 2022-09-13 14:09:04.595076

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0e66bcee6c96"
down_revision = "52702703ae24"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO direct_ingest_instance_status (region_code, timestamp, instance, status) VALUES
        ('US_OR', '2022-09-13T00:00:00.000000', 'PRIMARY', 'STANDARD_RERUN_STARTED'),
        ('US_OR', '2022-09-13T00:00:00.000000', 'SECONDARY', 'NO_RERUN_IN_PROGRESS');
    """
    )


def downgrade() -> None:
    op.execute(
        """
           DELETE FROM direct_ingest_instance_status
           WHERE region_code = 'US_OR';
       """
    )
