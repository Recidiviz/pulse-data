# pylint: skip-file
"""add_us_tx_initial_ingest_statuses

Revision ID: de369946ba7d
Revises: f64925b297f3
Create Date: 2024-08-26 14:22:04.545627

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "de369946ba7d"
down_revision = "f64925b297f3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
                INSERT INTO direct_ingest_instance_status (region_code, instance, status, status_timestamp) VALUES
                ('US_TX', 'PRIMARY', 'INITIAL_STATE', '2024-08-26 12:59:16.392675'),
                ('US_TX', 'SECONDARY', 'NO_RAW_DATA_REIMPORT_IN_PROGRESS', '2024-08-26 12:59:16.392675');
            """
    )


def downgrade() -> None:
    op.execute(
        """
                   DELETE FROM direct_ingest_instance_status
                   WHERE region_code = 'US_TX';
               """
    )
