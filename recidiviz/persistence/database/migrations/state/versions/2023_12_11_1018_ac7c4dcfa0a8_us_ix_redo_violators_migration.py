# pylint: skip-file
"""us_ix_redo_violators_migration

Revision ID: ac7c4dcfa0a8
Revises: 7f28f8767137
Create Date: 2023-12-11 10:18:15.819517

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ac7c4dcfa0a8"
down_revision = "7f28f8767137"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'WARRANT'
        WHERE
            state_code = 'US_IX'
            and supervision_level_raw_text in ('PAROLE VIOLATOR', 'PROBATION VIOLATOR');
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'INTERNAL_UNKNOWN'
        WHERE
            state_code = 'US_IX'
            and supervision_level_raw_text in ('PAROLE VIOLATOR', 'PROBATION VIOLATOR');
        """
    )
