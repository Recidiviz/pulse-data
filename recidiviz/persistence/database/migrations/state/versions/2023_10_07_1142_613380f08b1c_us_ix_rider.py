# pylint: skip-file
"""us_ix_rider

Revision ID: 613380f08b1c
Revises: d9a2156b4fe9
Create Date: 2023-10-07 11:42:46.470797

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "613380f08b1c"
down_revision = "d9a2156b4fe9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_period 
        SET specialized_purpose_for_incarceration = 'TREATMENT_IN_PRISON'
        WHERE 
            state_code = 'US_IX' 
            and custody_level_raw_text = 'RIDER';

        UPDATE state_incarceration_period 
        SET specialized_purpose_for_incarceration_raw_text = NULL
        WHERE 
            state_code = 'US_IX' 
            and custody_level_raw_text = 'RIDER';
        """
    )


def downgrade() -> None:
    pass
