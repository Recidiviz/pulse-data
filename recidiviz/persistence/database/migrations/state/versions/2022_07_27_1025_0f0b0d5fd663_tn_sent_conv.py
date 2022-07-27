# pylint: skip-file
"""tn_sent_conv

Revision ID: 0f0b0d5fd663
Revises: 7ca98360b9a1
Create Date: 2022-07-27 10:25:45.323633

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0f0b0d5fd663"
down_revision = "7ca98360b9a1"
branch_labels = None
depends_on = None


UPGRADE_QUERY = (
    "UPDATE state_charge SET status = 'CONVICTED' WHERE state_code = 'US_TN';"
)
DOWNGRADE_QUERY = (
    "UPDATE state_charge SET status = 'SENTENCED' WHERE state_code = 'US_TN';"
)


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
