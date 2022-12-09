# pylint: skip-file
"""set_housing_unit_type

Revision ID: 678713bbf8a2
Revises: e337bbdbe043
Create Date: 2022-12-09 10:50:46.221915

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "678713bbf8a2"
down_revision = "e337bbdbe043"
branch_labels = None
depends_on = None

UPDATE_QUERY = """
    UPDATE state_incarceration_period
    SET housing_unit_type_raw_text = housing_unit,
        housing_unit_type = 'GENERAL'
    WHERE state_code = 'US_ME' OR state_code = 'US_CO'
"""

DOWNGRADE_QUERY = """
    UPDATE state_incarceration_period
    SET housing_unit_type_raw_text = NULL, housing_unit_type = NULL
    WHERE state_code = 'US_ME' OR state_code = 'US_CO'
"""


def upgrade() -> None:
    op.execute(UPDATE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
