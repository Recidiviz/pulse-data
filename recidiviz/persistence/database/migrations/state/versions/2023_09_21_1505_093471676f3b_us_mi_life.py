# pylint: skip-file
"""us_mi_life

Revision ID: 093471676f3b
Revises: 2bfa9cac7e15
Create Date: 2023-09-21 15:05:09.953207

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "093471676f3b"
down_revision = "2bfa9cac7e15"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_sentence 
        SET is_life = True
        WHERE 
            state_code = 'US_MI' 
            and projected_max_release_date = '2999-12-31'
            and is_life = False;

        UPDATE state_supervision_sentence 
        SET is_life = True
        WHERE 
            state_code = 'US_MI' 
            and projected_completion_date = '2999-12-31'
            and is_life = False;
        """
    )


def downgrade() -> None:
    pass
