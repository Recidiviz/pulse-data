# pylint: skip-file
"""mo_remap_release_reason_enum_transfer

Revision ID: 33ed0b863047
Revises: 9c37ad604edd
Create Date: 2022-11-07 15:04:45.388917

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "33ed0b863047"
down_revision = "9c37ad604edd"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_period
        SET release_reason = 'TRANSFER'
        WHERE release_reason_raw_text in ('NONE@@NONE@@NONE')
        AND state_code = 'US_MO'
        """
    )
    pass
    # ### end Alembic commands ###


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_period
        SET release_reason = 'INTERNAL_UNKNOWN'
        WHERE release_reason_raw_text in ('NONE@@NONE@@NONE')
        AND state_code = 'US_MO'
        """
    )
    pass
    # ### end Alembic commands ###
