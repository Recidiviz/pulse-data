# pylint: skip-file
"""drop_sentence_groups_us_nd

Revision ID: 0b628c170756
Revises: 7d87d2442822
Create Date: 2021-12-21 12:08:21.460513

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "0b628c170756"
down_revision = "7d87d2442822"
branch_labels = None
depends_on = None

DELETE_QUERY = "DELETE FROM {table_name} WHERE state_code = 'US_ND';"

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
