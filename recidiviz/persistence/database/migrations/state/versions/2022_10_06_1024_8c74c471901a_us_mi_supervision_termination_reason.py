# pylint: skip-file
"""us_mi_supervision_termination_reason

Revision ID: 8c74c471901a
Revises: fd946cbe3966
Create Date: 2022-10-06 10:24:38.482174

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8c74c471901a"
down_revision = "fd946cbe3966"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET termination_reason = 'REVOCATION'
        WHERE termination_reason_raw_text in ('11', '12', '14', '15', '18', '158')
        AND state_code = 'US_MI'
        """
    )


def downgrade() -> None:

    op.execute(
        """
        UPDATE state_supervision_period
        SET termination_reason = 'ADMITTED_TO_INCARCERATION'
        WHERE termination_reason_raw_text in ('11', '12', '14', '15', '18', '158')
        AND state_code = 'US_MI'
        """
    )
