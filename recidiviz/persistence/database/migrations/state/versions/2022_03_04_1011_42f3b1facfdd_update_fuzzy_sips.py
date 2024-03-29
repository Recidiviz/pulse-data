# pylint: skip-file
"""update_fuzzy_sips

Revision ID: 42f3b1facfdd
Revises: 7c6df6895461
Create Date: 2022-03-04 10:11:04.560089

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "42f3b1facfdd"
down_revision = "7c6df6895461"
branch_labels = None
depends_on = None

UPGRADE_QUERY = """UPDATE {table}
SET {end_date} = {start_date},
{end_reason} = 'INTERNAL_UNKNOWN'
WHERE state_code = 'US_ID' AND external_id LIKE '%FUZZY_MATCHED'"""

DOWNGRADE_QUERY = """UPDATE {table}
SET {end_date} = NULL,
{end_reason} = NULL
WHERE state_code = 'US_ID' AND external_id LIKE '%FUZZY_MATCHED'"""

ENTRIES = [
    {
        "table": "state_incarceration_period",
        "end_date": "release_date",
        "start_date": "admission_date",
        "end_reason": "release_reason",
    },
    {
        "table": "state_supervision_period",
        "end_date": "termination_date",
        "start_date": "start_date",
        "end_reason": "termination_reason",
    },
]


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    for entry in ENTRIES:
        op.execute(StrictStringFormatter().format(UPGRADE_QUERY, **entry))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    for entry in ENTRIES:
        op.execute(
            StrictStringFormatter().format(
                DOWNGRADE_QUERY,
                table=entry["table"],
                end_date=entry["end_date"],
                end_reason=entry["end_reason"],
            )
        )
    # ### end Alembic commands ###
