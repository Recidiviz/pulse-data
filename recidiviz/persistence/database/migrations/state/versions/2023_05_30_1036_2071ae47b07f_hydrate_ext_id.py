# pylint: skip-file
"""hydrate_ext_id

Revision ID: 2071ae47b07f
Revises: da61d6fdb049
Create Date: 2023-05-30 10:36:41.395338

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2071ae47b07f"
down_revision = "da61d6fdb049"
branch_labels = None
depends_on = None

UPGRADE_QUERY = """
    UPDATE state_supervision_violation_response
    SET external_id =  state_supervision_violation.external_id
    FROM state_supervision_violation
    WHERE state_supervision_violation_response.supervision_violation_id = state_supervision_violation.supervision_violation_id
    AND state_supervision_violation_response.state_code = 'US_ND'
"""

DOWNGRADE_QUERY = """
    UPDATE state_supervision_violation_response SET external_id = NULL WHERE state_code = 
    'US_ND';
"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
