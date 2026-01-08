# pylint: skip-file
"""add_us_ne_initial_ingest_statuses

Revision ID: d429094710bb
Revises: c9ba42826ba7
Create Date: 2024-07-30 18:40:56.944771

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d429094710bb"
down_revision = "c9ba42826ba7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
                INSERT INTO direct_ingest_instance_status (region_code, instance, status, status_timestamp) VALUES
                ('US_NE', 'PRIMARY', 'INITIAL_STATE', '2024-07-30 12:59:16.392675'),
                ('US_NE', 'SECONDARY', 'NO_RAW_DATA_REIMPORT_IN_PROGRESS', '2024-07-30 12:59:16.392675');
            """
    )


def downgrade() -> None:
    op.execute(
        """
                   DELETE FROM direct_ingest_instance_status
                   WHERE region_code = 'US_NE';
               """
    )
