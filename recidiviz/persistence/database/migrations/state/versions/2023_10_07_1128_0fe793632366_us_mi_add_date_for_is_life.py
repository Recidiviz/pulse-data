# pylint: skip-file
"""us_mi_add_date_for_is_life

Revision ID: 0fe793632366
Revises: e1fd9273a93c
Create Date: 2023-10-07 11:28:12.215075

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0fe793632366"
down_revision = "227714f1a0ff"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_sentence 
        SET is_life = True
        WHERE 
            state_code = 'US_MI' 
            and projected_max_release_date = '9999-12-31'
            and is_life = False;

        UPDATE state_supervision_sentence 
        SET is_life = True
        WHERE 
            state_code = 'US_MI' 
            and projected_completion_date = '9999-12-31'
            and is_life = False;
        """
    )


def downgrade() -> None:
    pass
