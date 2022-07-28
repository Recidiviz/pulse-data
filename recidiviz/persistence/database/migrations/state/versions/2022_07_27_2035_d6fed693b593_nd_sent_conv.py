# pylint: skip-file
"""nd_sent_conv

Revision ID: d6fed693b593
Revises: a9ba60da38a6
Create Date: 2022-07-27 20:35:25.176638

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d6fed693b593"
down_revision = "a9ba60da38a6"
branch_labels = None
depends_on = None


UPGRADE_QUERY = "UPDATE state_charge SET status = 'CONVICTED', status_raw_text = 'CONVICTED' WHERE state_code = 'US_ND' AND status_raw_text = 'SENTENCED';"
DOWNGRADE_QUERY = "UPDATE state_charge SET status = 'SENTENCED', status_raw_text = 'SENTENCED' WHERE state_code = 'US_ND' AND status_raw_text = 'SENTENCED';"


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
