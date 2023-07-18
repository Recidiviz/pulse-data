# pylint: skip-file
"""change_tn_6P_sup_levels

Revision ID: f79f447d3600
Revises: be338384da3d
Create Date: 2023-07-18 11:27:06.265682

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f79f447d3600"
down_revision = "be338384da3d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period SET supervision_level = 'MEDIUM'
        WHERE state_code = 'US_TN' AND supervision_level_raw_text IN ('6P1', '6P2', '6P4');
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period SET supervision_level = 'INTERNAL_UNKNOWN'
        WHERE state_code = 'US_TN' AND supervision_level_raw_text IN ('6P1', '6P2', '6P4');
        """
    )
