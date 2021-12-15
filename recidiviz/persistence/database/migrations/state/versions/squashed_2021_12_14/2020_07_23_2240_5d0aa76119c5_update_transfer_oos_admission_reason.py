# pylint: skip-file
"""update_transfer_oos_admission_reason

Revision ID: 5d0aa76119c5
Revises: e5949964b987
Create Date: 2020-07-23 22:40:28.347075

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "5d0aa76119c5"
down_revision = "e5949964b987"
branch_labels = None
depends_on = None

ADMISSION_REASON_QUERY = (
    "SELECT incarceration_period_id FROM"
    " state_incarceration_period"
    " WHERE state_code = 'US_ND' AND admission_reason_raw_text = 'OOS'"
)


UPDATE_QUERY = (
    "UPDATE state_incarceration_period SET admission_reason = '{new_value}'"
    " WHERE incarceration_period_id IN ({ids_query});"
)


def upgrade() -> None:
    connection = op.get_bind()

    updated_admission_reason = "TRANSFERRED_FROM_OUT_OF_STATE"

    connection.execute(
        StrictStringFormatter().format(
            UPDATE_QUERY,
            new_value=updated_admission_reason,
            ids_query=ADMISSION_REASON_QUERY,
        )
    )


def downgrade() -> None:
    connection = op.get_bind()

    updated_admission_reason = "TRANSFER"

    connection.execute(
        StrictStringFormatter().format(
            UPDATE_QUERY,
            new_value=updated_admission_reason,
            ids_query=ADMISSION_REASON_QUERY,
        )
    )
