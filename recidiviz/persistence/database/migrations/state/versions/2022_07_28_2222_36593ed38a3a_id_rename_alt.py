# pylint: skip-file
"""id_rename_alt

Revision ID: 36593ed38a3a
Revises: 5d5b3de2fbfe
Create Date: 2022-07-28 22:22:17.337748

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "36593ed38a3a"
down_revision = "5d5b3de2fbfe"
branch_labels = None
depends_on = None

UPGRADE_QUERY = """UPDATE state_supervision_contact SET location = 'ALTERNATIVE_PLACE_OF_EMPLOYMENT' 
WHERE state_code = 'US_ID' AND location_raw_text = 'ALTERNATE WORK SITE';"""

DOWNGRADE_QUERY = """UPDATE state_supervision_contact SET location = 'ALTERNATIVE_WORK_SITE' 
WHERE state_code = 'US_ID' AND location_raw_text = 'ALTERNATE WORK SITE';"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
