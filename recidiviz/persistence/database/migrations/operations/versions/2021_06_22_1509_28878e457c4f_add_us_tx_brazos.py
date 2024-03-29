# pylint: skip-file
"""add_us_tx_brazos

Revision ID: 28878e457c4f
Revises: 26ab0f729241
Create Date: 2021-06-22 15:09:37.917897

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "28878e457c4f"
down_revision = "26ab0f729241"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO direct_ingest_instance_status (region_code, instance, is_paused) VALUES
        ('US_TX_BRAZOS', 'PRIMARY', TRUE),
        ('US_TX_BRAZOS', 'SECONDARY', TRUE);
    """
    )


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
