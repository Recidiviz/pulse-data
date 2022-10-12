# pylint: skip-file
"""us_mi_incarceration_admission_reasons

Revision ID: d449b1d3714e
Revises: fd946cbe3966
Create Date: 2022-10-05 14:53:59.777758

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d449b1d3714e"
down_revision = "8c74c471901a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_period
        SET admission_reason = 'REVOCATION'
        WHERE admission_reason_raw_text in ('11', '12', '14', '15')
        AND state_code = 'US_MI'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_period
        SET admission_reason = 'NEW_ADMISSION'
        WHERE admission_reason_raw_text in ('11', '12', '14', '15')
        AND state_code = 'US_MI'
        """
    )
