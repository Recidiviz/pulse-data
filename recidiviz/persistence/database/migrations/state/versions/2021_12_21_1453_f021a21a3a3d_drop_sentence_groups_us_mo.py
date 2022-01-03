# pylint: skip-file
"""drop_sentence_groups_us_mo

Revision ID: f021a21a3a3d
Revises: a799d5d5b2bb
Create Date: 2021-12-21 14:53:05.383944

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "f021a21a3a3d"
down_revision = "a799d5d5b2bb"
branch_labels = None
depends_on = None


DELETE_QUERY = "DELETE FROM {table_name} WHERE state_code = 'US_MO';"

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
