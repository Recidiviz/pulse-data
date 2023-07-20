# pylint: skip-file
"""remove_tn_ped_hydration

Revision ID: 5b5d6d1f42a0
Revises: 43640a2cfdf4
Create Date: 2023-07-19 16:04:22.347843

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5b5d6d1f42a0"
down_revision = "43640a2cfdf4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_sentence SET parole_eligibility_date = NULL
        WHERE state_code = 'US_TN';
        """
    )


def downgrade() -> None:
    # There is no downgrade migration for this migration
    pass
