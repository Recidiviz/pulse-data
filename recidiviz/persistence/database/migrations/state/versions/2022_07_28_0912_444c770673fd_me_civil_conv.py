# pylint: skip-file
"""me_civil_conv

Revision ID: 444c770673fd
Revises: ddaf417ade56
Create Date: 2022-07-28 09:12:23.790635

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "444c770673fd"
down_revision = "ddaf417ade56"
branch_labels = None
depends_on = None

UPGRADE_QUERY = """UPDATE state_charge SET classification_type = 'CIVIL' WHERE state_code
 = 'US_ME' AND classification_type_raw_text = 'T';"""
DOWNGRADE_QUERY = """UPDATE state_charge SET classification_type = 'INFRACTION' WHERE 
state_code = 'US_ME' AND classification_type_raw_text = 'T';"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
