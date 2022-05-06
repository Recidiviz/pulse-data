# pylint: skip-file
"""us_me_charge_fix

Revision ID: a6a86d0718db
Revises: 6d2226525eb4
Create Date: 2022-05-05 10:22:09.175135

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "a6a86d0718db"
down_revision = "09a00a11ebcb"
branch_labels = None
depends_on = None


UPDATE_QUERY = (
    "UPDATE {table_name} SET classification_type = '{new_value}'"
    " WHERE state_code = 'US_ME'"
    " AND classification_type = '{old_value}';"
)

TABLES = [
    "state_charge",
    "state_charge_history",
]


def upgrade() -> None:
    old_value = "OTHER"
    new_value = "INFRACTION"

    for table in TABLES:
        op.execute(
            StrictStringFormatter().format(
                UPDATE_QUERY,
                table_name=table,
                new_value=new_value,
                old_value=old_value,
            )
        )


def downgrade() -> None:
    # NOTE: THIS IS NOT A PERFECT DOWNGRADE.
    # There are some `INFRACTION` instances that will be changed to `OTHER` in this
    # downgrade that would not have been mapped to `OTHER` in ingest. However,
    # it is not possible to distinguish between the StateCharge entities that
    # originated from the us_me_supervision_sentences view versus the
    # us_me_incarceration_sentences view. As of 5/5/2022, this will only impact 1
    # StateCharge entity.

    old_value = "INFRACTION"
    new_value = "OTHER"

    for table in TABLES:
        op.execute(
            StrictStringFormatter().format(
                UPDATE_QUERY,
                table_name=table,
                new_value=new_value,
                old_value=old_value,
            )
        )
