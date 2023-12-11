# pylint: skip-file
"""us_mi_admin_supervision_remap

Revision ID: 6eed48eb6ff1
Revises: a2eafc022f5e
Create Date: 2023-12-11 10:54:38.833328

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6eed48eb6ff1"
down_revision = "a2eafc022f5e"
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
            and supervision_level_raw_text in ('2292', '3624');

        UPDATE state_supervision_period
        SET supervision_level = 'IN_CUSTODY'
        WHERE
            state_code = 'US_MI'
            and supervision_level = 'INTERNAL_UNKNOWN'
            and supervision_level_raw_text in ('7481');
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
            and supervision_level_raw_text in ('2292', '3624');

        UPDATE state_supervision_period
        SET supervision_level = 'INTERNAL_UNKNOWN'
        WHERE
            state_code = 'US_MI'
            and supervision_level = 'IN_CUSTODY'
            and supervision_level_raw_text in ('7481');
        """
    )
