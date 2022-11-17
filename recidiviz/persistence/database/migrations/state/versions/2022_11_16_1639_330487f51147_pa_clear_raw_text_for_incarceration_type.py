# pylint: skip-file
"""pa_clear_raw_text_for_incarceration_type

Revision ID: 330487f51147
Revises: 33ed0b863047
Create Date: 2022-11-16 16:39:00.953165

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "330487f51147"
down_revision = "33ed0b863047"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "UPDATE state_incarceration_period SET incarceration_type_raw_text = NULL WHERE state_code = 'US_PA' AND incarceration_type = 'COUNTY_JAIL';"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE state_incarceration_period SET incarceration_type_raw_text = 'CCIS' WHERE state_code = 'US_PA' AND incarceration_type = 'COUNTY_JAIL';"
    )
