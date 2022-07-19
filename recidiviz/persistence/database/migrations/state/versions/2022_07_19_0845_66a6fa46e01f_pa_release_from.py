# pylint: skip-file
"""pa_release_from

Revision ID: 66a6fa46e01f
Revises: 7a633f8d80d3
Create Date: 2022-07-19 08:45:39.660759

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "66a6fa46e01f"
down_revision = "7a633f8d80d3"
branch_labels = None
depends_on = None


UPGRADE_QUERY = """
UPDATE state_supervision_period SET admission_reason = 'RELEASE_FROM_INCARCERATION' WHERE state_code = 
'US_PA' AND admission_reason_raw_text IN ('02', 'B2', 'R2', 'C2', '03', 'R3', 'C3');
"""

DOWNGRADE_QUERY = """
UPDATE state_supervision_period SET admission_reason = 'CONDITIONAL_RELEASE' WHERE state_code = 
'US_PA' AND admission_reason_raw_text IN ('02', 'B2', 'R2', 'C2', '03', 'R3', 'C3');
"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
