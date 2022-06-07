# pylint: skip-file
"""us_me_absconsion_periods

Revision ID: 0ea507d4ebe0
Revises: b732cc30fd91
Create Date: 2022-06-06 15:15:32.477208

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "0ea507d4ebe0"
down_revision = "b732cc30fd91"
branch_labels = None
depends_on = None


UPDATE_QUERY = (
    "UPDATE state_supervision_period SET supervision_type = '{new_value}'"
    " WHERE state_code = 'US_ME'"
    " and supervision_type_raw_text LIKE '%%ESCAPE%%';"
)


def upgrade() -> None:
    new_value = "ABSCONSION"

    op.execute(
        StrictStringFormatter().format(
            UPDATE_QUERY,
            new_value=new_value,
        )
    )


def downgrade() -> None:
    new_value = "PROBATION"

    op.execute(
        StrictStringFormatter().format(
            UPDATE_QUERY,
            new_value=new_value,
        )
    )
