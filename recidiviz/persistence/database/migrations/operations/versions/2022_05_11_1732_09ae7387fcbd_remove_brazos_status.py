# pylint: skip-file
"""remove_brazos_status

Revision ID: 09ae7387fcbd
Revises: 0e281321e43d
Create Date: 2022-05-11 17:32:31.106967

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "09ae7387fcbd"
down_revision = "0e281321e43d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "DELETE FROM direct_ingest_instance_status WHERE region_code = 'US_TX_BRAZOS';"
    )


def downgrade() -> None:
    op.execute(
        """
        INSERT INTO direct_ingest_instance_status (region_code, instance, is_paused) VALUES
        ('US_TX_BRAZOS', 'PRIMARY', TRUE),
        ('US_TX_BRAZOS', 'SECONDARY', TRUE);"""
    )
