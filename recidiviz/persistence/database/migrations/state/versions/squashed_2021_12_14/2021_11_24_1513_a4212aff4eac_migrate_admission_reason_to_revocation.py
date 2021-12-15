# pylint: skip-file
"""migrate_admission_reason_to_revocation

Revision ID: a4212aff4eac
Revises: e13668b71ccd
Create Date: 2021-11-24 15:13:40.345972

"""
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "a4212aff4eac"
down_revision = "e13668b71ccd"
branch_labels = None
depends_on = None

# Need to explicitly cast admission reason as string in order to use LIKE.
ENUM_VALUE_SELECT_IDS_QUERY = (
    "SELECT incarceration_period_id FROM {table_name}"
    " WHERE admission_reason::text LIKE '%REVOCATION'"
)

UPDATE_QUERY = (
    "UPDATE {table_name} SET admission_reason = 'REVOCATION'"
    " WHERE incarceration_period_id IN ({ids_query});"
)

TABLE_NAMES = ["state_incarceration_period", "state_incarceration_period_history"]


def upgrade() -> None:
    for table_name in TABLE_NAMES:
        op.execute(
            StrictStringFormatter().format(
                UPDATE_QUERY,
                table_name=table_name,
                ids_query=StrictStringFormatter().format(
                    ENUM_VALUE_SELECT_IDS_QUERY,
                    table_name=table_name,
                ),
            )
        )


def downgrade() -> None:
    # It is not possible to run a downgrade for this migration. In order to simulate a downgrade, a backup would
    # need to be restored.
    pass
