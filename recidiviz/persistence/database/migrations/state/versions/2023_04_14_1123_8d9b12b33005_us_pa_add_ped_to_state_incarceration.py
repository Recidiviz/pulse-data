# pylint: skip-file
"""us_pa_add_ped_to_state_incarceration

Revision ID: 8d9b12b33005
Revises: 99807d598d3e
Create Date: 2023-04-14 11:23:38.092486

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8d9b12b33005"
down_revision = "99807d598d3e"
branch_labels = None
depends_on = None

UPGRADE_QUERY = """
    UPDATE state_incarceration_sentence
    SET parole_eligibility_date = projected_min_release_date
    WHERE state_code = 'US_PA';

"""

DOWNGRADE_QUERY = """
    UPDATE state_incarceration_sentence
    SET parole_eligibility_date = NULL
    WHERE state_code = 'US_PA';
"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
