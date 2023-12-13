# pylint: skip-file
"""us_tn_remap_9dp_to_deported

Revision ID: b5305c88e6da
Revises: 965f4591141f
Create Date: 2023-12-13 16:35:20.111161

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b5305c88e6da"
down_revision = "965f4591141f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_type = 'DEPORTED'
        WHERE
            state_code = 'US_TN'
            and supervision_type_raw_text like '9DP%';
        """
    )


def downgrade() -> None:
    # There is no downgrade migration for this migration
    pass
