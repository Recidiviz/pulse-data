# pylint: skip-file
"""US_CA_remap_PAL

Revision ID: 68e07d95ef0a
Revises: 1d9471924ec6
Create Date: 2023-06-09 21:27:20.850100

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "68e07d95ef0a"
down_revision = "1d9471924ec6"
branch_labels = None
depends_on = None

UPGRADE_QUERY = """
UPDATE state_person
SET residency_status = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_CA'
    AND residency_status_raw_text = 'PAL REPORT SUBMITTED';
"""

DOWNGRADE_QUERY = """
UPDATE state_person
SET residency_status = 'EXTERNAL_UNKNOWN'
WHERE state_code = 'US_CA'
    AND residency_status_raw_text = 'PAL REPORT SUBMITTED';
"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
