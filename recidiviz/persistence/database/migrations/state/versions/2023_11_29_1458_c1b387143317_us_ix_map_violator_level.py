# pylint: skip-file
"""us_ix_map_violator_level

Revision ID: c1b387143317
Revises: 613380f08b1c
Create Date: 2023-11-29 14:58:40.624956

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c1b387143317"
down_revision = "613380f08b1c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    UPDATE state_supervision_period
    SET supervision_level = 'WARRANT'
    WHERE
        state_code = 'US_IX'
        and supervision_level_raw_text in ('PAROLE VIOLATOR', 'PROBATION VIOLATOR');
    """


def downgrade() -> None:
    """
    UPDATE state_supervision_period
    SET supervision_level = 'INTERNAL_UNKNOWN'
    WHERE
        state_code = 'US_IX'
        and supervision_level_raw_text in ('PAROLE VIOLATOR', 'PROBATION VIOLATOR');
    """
