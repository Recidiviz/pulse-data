# pylint: skip-file
"""remove_us_nd_probation_raw_text

Revision ID: 771022ce2154
Revises: e2a5093ac219
Create Date: 2021-12-15 13:40:53.525150

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "771022ce2154"
down_revision = "e2a5093ac219"
branch_labels = None
depends_on = None


ENUM_VALUE_SELECT_IDS_QUERY = (
    "SELECT supervision_sentence_id FROM {table_name}"
    " WHERE state_code = 'US_ND' AND supervision_type_raw_text = 'PROBATION'"
)

DELETE_QUERY = (
    "DELETE FROM {table_name} WHERE supervision_sentence_id IN ({ids_query});"
)

TABLE_NAMES = ["state_supervision_sentence_history", "state_supervision_sentence"]


def upgrade() -> None:
    for table_name in TABLE_NAMES:
        op.execute(
            StrictStringFormatter().format(
                DELETE_QUERY,
                table_name=table_name,
                ids_query=StrictStringFormatter().format(
                    ENUM_VALUE_SELECT_IDS_QUERY,
                    table_name=table_name,
                ),
            )
        )


def downgrade() -> None:
    # This migration does not have a downgrade
    pass
