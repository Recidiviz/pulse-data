# pylint: skip-file
"""add_us_me

Revision ID: a42a4d0ad7a9
Revises: 672091849dfe
Create Date: 2021-09-20 16:17:47.854100

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a42a4d0ad7a9"
down_revision = "672091849dfe"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO direct_ingest_instance_status (region_code, instance, is_paused) VALUES
        ('US_ME', 'PRIMARY', TRUE),
        ('US_ME', 'SECONDARY', TRUE);
    """
    )


def downgrade() -> None:
    op.execute(
        """
            DELETE FROM direct_ingest_instance_status
            WHERE region_code = 'US_ME';
        """
    )
