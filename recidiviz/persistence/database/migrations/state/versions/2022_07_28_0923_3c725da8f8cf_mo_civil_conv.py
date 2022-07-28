# pylint: skip-file
"""mo_civil_conv

Revision ID: 3c725da8f8cf
Revises: 444c770673fd
Create Date: 2022-07-28 09:23:28.085405

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3c725da8f8cf"
down_revision = "444c770673fd"
branch_labels = None
depends_on = None

UPGRADE_QUERY = """UPDATE state_charge SET classification_type = 'CIVIL' WHERE 
state_code = 'US_MO' AND classification_type_raw_text IN ('L', 'I');"""
DOWNGRADE_QUERY = """UPDATE state_charge SET classification_type = 'INFRACTION' WHERE 
state_code = 'US_MO' AND classification_type_raw_text IN ('L', 'I');"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
