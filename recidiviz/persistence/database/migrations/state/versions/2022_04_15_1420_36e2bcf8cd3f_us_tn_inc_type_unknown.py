# pylint: skip-file
"""us_tn_inc_type_unknown

Revision ID: 36e2bcf8cd3f
Revises: 189e82474703
Create Date: 2022-04-15 14:20:16.992336

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "36e2bcf8cd3f"
down_revision = "189e82474703"
branch_labels = None
depends_on = None


UPDATE_QUERY = (
    "UPDATE {table_name} SET incarceration_type = '{updated_value}'"
    " WHERE state_code = 'US_TN' and incarceration_type_raw_text = 'CC';"
)

TABLES = ["state_incarceration_sentence", "state_incarceration_sentence_history"]


def upgrade() -> None:
    new_incarceration_type = "INTERNAL_UNKNOWN"

    for table in TABLES:
        op.execute(
            StrictStringFormatter().format(
                UPDATE_QUERY,
                table_name=table,
                updated_value=new_incarceration_type,
            )
        )


def downgrade() -> None:
    new_incarceration_type = "EXTERNAL_UNKNOWN"

    for table in TABLES:
        op.execute(
            StrictStringFormatter().format(
                UPDATE_QUERY,
                table_name=table,
                updated_value=new_incarceration_type,
            )
        )
