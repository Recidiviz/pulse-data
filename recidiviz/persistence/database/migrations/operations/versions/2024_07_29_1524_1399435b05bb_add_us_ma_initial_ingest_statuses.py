# pylint: skip-file
"""add_us_ma_initial_ingest_statuses

Revision ID: 1399435b05bb
Revises: e04bac966734
Create Date: 2024-07-29 15:24:40.842977

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1399435b05bb"
down_revision = "e04bac966734"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
                INSERT INTO direct_ingest_instance_status (region_code, instance, status, status_timestamp) VALUES
                ('US_MA', 'PRIMARY', 'INITIAL_STATE', '2024-07-29T00:00:00.000000'),
                ('US_MA', 'SECONDARY', 'NO_RAW_DATA_REIMPORT_IN_PROGRESS', '2024-07-29T00:00:00.000000');
            """
    )


def downgrade() -> None:
    op.execute(
        """
                   DELETE FROM direct_ingest_instance_status
                   WHERE region_code = 'US_MA';
               """
    )
