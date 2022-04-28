# pylint: skip-file
"""drop_nd_sorac

Revision ID: 2fe9deb1d56f
Revises: a29bd2a64aa6
Create Date: 2022-04-27 16:20:27.914264

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "2fe9deb1d56f"
down_revision = "036740faa838"
branch_labels = None
depends_on = None

DELETE_QUERY = """
DELETE FROM {table_name} WHERE assessment_id IN (
    SELECT assessment_id FROM state_assessment
    WHERE state_code = 'US_ND' AND assessment_type = 'SORAC'
);"""

TABLE_NAMES = ["state_assessment_history", "state_assessment"]


def upgrade() -> None:
    for table_name in TABLE_NAMES:
        op.execute(StrictStringFormatter().format(DELETE_QUERY, table_name=table_name))


def downgrade() -> None:
    # This migration does not have a downgrade
    pass
