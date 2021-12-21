# pylint: skip-file
"""drop_sentence_groups_us_id

Revision ID: a799d5d5b2bb
Revises: 0b628c170756
Create Date: 2021-12-21 13:41:53.517599

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "a799d5d5b2bb"
down_revision = "0b628c170756"
branch_labels = None
depends_on = None


DELETE_QUERY = "DELETE FROM {table_name} WHERE state_code = 'US_ID';"

TABLE_NAMES = ["state_sentence_group_history", "state_sentence_group"]


def upgrade() -> None:
    for table_name in TABLE_NAMES:
        op.execute(
            StrictStringFormatter().format(
                DELETE_QUERY,
                table_name=table_name,
            )
        )


def downgrade() -> None:
    # This migration does not have a downgrade
    pass
