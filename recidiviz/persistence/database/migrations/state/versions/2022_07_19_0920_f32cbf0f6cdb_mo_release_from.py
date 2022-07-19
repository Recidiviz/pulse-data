# pylint: skip-file
"""mo_release_from

Revision ID: f32cbf0f6cdb
Revises: 66a6fa46e01f
Create Date: 2022-07-19 09:20:18.577532

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f32cbf0f6cdb"
down_revision = "66a6fa46e01f"
branch_labels = None
depends_on = None


UPGRADE_QUERY = """
UPDATE state_supervision_period SET admission_reason = 'RELEASE_FROM_INCARCERATION' WHERE state_code = 
'US_MO' AND (admission_reason_raw_text LIKE '40O%' OR admission_reason_raw_text LIKE '%,40O%');
"""

DOWNGRADE_QUERY = """
UPDATE state_supervision_period SET admission_reason = 'CONDITIONAL_RELEASE' WHERE state_code = 
'US_MO' AND (admission_reason_raw_text LIKE '40O%' OR admission_reason_raw_text LIKE '%,40O%');
"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
