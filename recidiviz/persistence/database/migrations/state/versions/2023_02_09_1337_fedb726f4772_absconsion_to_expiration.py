# pylint: skip-file
"""absconsion_to_expiration

Revision ID: fedb726f4772
Revises: 303c73931475
Create Date: 2023-02-09 13:37:59.771756

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "fedb726f4772"
down_revision = "303c73931475"
branch_labels = None
depends_on = None

UPGRADE_QUERY = """
    UPDATE state_supervision_period SET termination_reason = 'EXPIRATION' WHERE state_code = 
    'US_PA' AND termination_reason_raw_text = '45';
"""

DOWNGRADE_QUERY = """
    UPDATE state_supervision_period SET termination_reason = 'ABSCONSION' WHERE state_code = 
    'US_PA' AND termination_reason_raw_text = '45';
"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
