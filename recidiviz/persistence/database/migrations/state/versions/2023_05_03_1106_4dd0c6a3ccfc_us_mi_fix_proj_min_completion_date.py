# pylint: skip-file
"""us_mi_fix_proj_min_completion_date

Revision ID: 4dd0c6a3ccfc
Revises: 9211a1fa6315
Create Date: 2023-05-03 11:06:03.662399

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "4dd0c6a3ccfc"
down_revision = "9211a1fa6315"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    UPDATE state_incarceration_sentence
    SET projected_min_release_date = parole_eligibility_date
    WHERE state_code = 'US_MI'
    """


def downgrade() -> None:
    """
    UPDATE state_incarceration_sentence
    SET projected_min_release_date = NULL
    WHERE state_code = 'US_MI'
    """
