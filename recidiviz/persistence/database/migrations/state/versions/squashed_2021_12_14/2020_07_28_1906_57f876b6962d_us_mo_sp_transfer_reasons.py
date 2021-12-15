# pylint: skip-file
"""us_mo_sp_transfer_reasons

Revision ID: 57f876b6962d
Revises: e5949964b987
Create Date: 2020-07-28 19:06:50.511026

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "57f876b6962d"
down_revision = "0aeeb9565367"
branch_labels = None
depends_on = None

UPGRADE_REASON_QUERY = (
    "SELECT supervision_period_id FROM state_supervision_period "
    "WHERE state_code = 'US_MO' AND {date_field_name} IS NOT NULL "
    "    AND {reason_field_name} IS NULL AND {reason_field_name}_raw_text IS NULL"
)

UPGRADE_UPDATE_QUERY = (
    "UPDATE state_supervision_period "
    "SET {reason_field_name} = 'TRANSFER_WITHIN_STATE', "
    "    {reason_field_name}_raw_text = 'TRANSFER_WITHIN_STATE' "
    "WHERE supervision_period_id IN ({ids_query});"
)


DOWNGRADE_REASON_QUERY = (
    "SELECT supervision_period_id FROM"
    " state_supervision_period"
    " WHERE state_code = 'US_MO' AND {date_field_name} IS NOT NULL"
    "   AND {reason_field_name} = 'TRANSFER_WITHIN_STATE'"
    "   AND {reason_field_name}_raw_text = 'TRANSFER_WITHIN_STATE'"
)

DOWNGRADE_UPDATE_QUERY = (
    "UPDATE state_supervision_period "
    "SET {reason_field_name} = NULL, "
    "    {reason_field_name}_raw_text = NULL "
    "WHERE supervision_period_id IN ({ids_query});"
)


def upgrade() -> None:
    connection = op.get_bind()

    admission_reason_upgrade_query = StrictStringFormatter().format(
        UPGRADE_UPDATE_QUERY,
        reason_field_name="admission_reason",
        ids_query=StrictStringFormatter().format(
            UPGRADE_REASON_QUERY,
            date_field_name="start_date",
            reason_field_name="admission_reason",
        ),
    )

    termination_reason_upgrade_query = StrictStringFormatter().format(
        UPGRADE_UPDATE_QUERY,
        reason_field_name="termination_reason",
        ids_query=StrictStringFormatter().format(
            UPGRADE_REASON_QUERY,
            date_field_name="termination_date",
            reason_field_name="termination_reason",
        ),
    )

    connection.execute(admission_reason_upgrade_query)
    connection.execute(termination_reason_upgrade_query)


def downgrade() -> None:
    connection = op.get_bind()

    admission_reason_downgrade_query = StrictStringFormatter().format(
        DOWNGRADE_UPDATE_QUERY,
        reason_field_name="admission_reason",
        ids_query=StrictStringFormatter().format(
            DOWNGRADE_REASON_QUERY,
            date_field_name="start_date",
            reason_field_name="admission_reason",
        ),
    )

    termination_reason_downgrade_query = StrictStringFormatter().format(
        DOWNGRADE_UPDATE_QUERY,
        reason_field_name="termination_reason",
        ids_query=StrictStringFormatter().format(
            DOWNGRADE_REASON_QUERY,
            date_field_name="termination_date",
            reason_field_name="termination_reason",
        ),
    )

    connection.execute(admission_reason_downgrade_query)
    connection.execute(termination_reason_downgrade_query)
