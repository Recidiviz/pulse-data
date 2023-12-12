# pylint: skip-file
"""us_mi_admin_sup_coms

Revision ID: 15f9c8e64103
Revises: fc742ceaa1ad
Create Date: 2023-12-11 17:31:56.218335

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "15f9c8e64103"
down_revision = "fc742ceaa1ad"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'UNSUPERVISED'
        WHERE
            state_code = 'US_MI'
            and supervision_level = 'IN_CUSTODY'
            and supervision_level_raw_text like '%MINIMUM ADMINISTRATIVE%';
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'IN_CUSTODY'
        WHERE
            state_code = 'US_MI'
            and supervision_level = 'UNSUPERVISED'
            and supervision_level_raw_text like '%MINIMUM ADMINISTRATIVE%';
        """
    )
