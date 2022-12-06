# pylint: skip-file
"""set_condition_to_internal_unknown

Revision ID: 7e70b52d2720
Revises: 6530bcb6cb62
Create Date: 2022-12-05 16:37:07.187912

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7e70b52d2720"
down_revision = "6530bcb6cb62"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_violated_condition_entry
        SET condition = 'INTERNAL_UNKNOWN'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_violated_condition_entry
        SET condition = NULL
        """
    )
