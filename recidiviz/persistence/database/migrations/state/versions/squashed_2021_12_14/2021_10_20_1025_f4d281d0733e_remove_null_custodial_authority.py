# pylint: skip-file
"""remove_null_custodial_authority

Revision ID: f4d281d0733e
Revises: 69d481f0a061
Create Date: 2021-10-20 10:25:38.411599

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "f4d281d0733e"
down_revision = "69d481f0a061"
branch_labels = None
depends_on = None

DELETE_QUERY = (
    "DELETE FROM {table_name}"
    " WHERE incarceration_period_id IN ("
    " SELECT incarceration_period_id"
    " FROM state_incarceration_period"
    " WHERE custodial_authority_raw_text IS NULL"
    " AND state_code = 'US_PA'"
    " AND incarceration_type_raw_text = 'CCIS');"
)

TABLES_TO_UPDATE = [
    "state_supervision_sentence_incarceration_period_association",
    "state_incarceration_sentence_incarceration_period_association",
    # Must delete from history table first to avoid violating foreign key constraint
    "state_incarceration_period_history",
    "state_incarceration_period",
]


def upgrade() -> None:
    for table in TABLES_TO_UPDATE:
        op.execute(StrictStringFormatter().format(DELETE_QUERY, table_name=table))


def downgrade() -> None:
    # Row deletion cannot be undone
    pass
