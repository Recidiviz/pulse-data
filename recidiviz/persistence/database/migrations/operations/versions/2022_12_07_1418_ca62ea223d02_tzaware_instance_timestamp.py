# pylint: skip-file
"""tzaware_instance_timestamp

Revision ID: ca62ea223d02
Revises: f27dbaf25678
Create Date: 2022-12-07 14:18:42.647444

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "ca62ea223d02"
down_revision = "f27dbaf25678"
branch_labels = None
depends_on = None

# Inspired by this answer: https://stackoverflow.com/a/68411764
UPGRADE_QUERY = f"""
ALTER TABLE direct_ingest_instance_status ALTER COLUMN timestamp TYPE TIMESTAMPTZ USING timestamp AT TIME ZONE 'UTC';
"""

DOWNGRADE_QUERY = f"""
ALTER TABLE direct_ingest_instance_status ALTER COLUMN timestamp TYPE TIMESTAMP USING timestamp AT TIME ZONE 'UTC';
"""


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(DOWNGRADE_QUERY)
